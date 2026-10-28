<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\BedrockError;
use Expensify\Bedrock\Exceptions\CircuitBreakerOpen;
use Expensify\Bedrock\Exceptions\ConnectionFailure;
use Expensify\Bedrock\Exceptions\Jobs\DoesNotExist;
use Expensify\Bedrock\Exceptions\Jobs\IllegalAction;
use Expensify\Bedrock\Exceptions\Jobs\RetryableException;
use Expensify\Bedrock\Exceptions\TimeoutError;
use Expensify\Bedrock\JobOutcomeRetrier;
use Expensify\Bedrock\Jobs;
use Expensify\Bedrock\Stats\NullStats;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;
use RuntimeException;
use Throwable;

class JobOutcomeRetrierTest extends TestCase
{
    private const COMMIT_LOCK_TIMEOUT = 'Internal Bedrock command timeout (555 Timeout waiting for commit lock)';
    private const BREAKER_OPEN = 'Bedrock circuit breaker open for cluster webrock (default)';

    /** @var int[] Seconds the retrier asked to wait, in order */
    private array $waits = [];

    private RecordingLogger $logger;

    private JobOutcomeRetrier $retrier;

    protected function setUp(): void
    {
        $this->waits = [];
        $this->logger = new RecordingLogger();
        $this->retrier = new JobOutcomeRetrier($this->logger, function (int $seconds): void {
            $this->waits[] = $seconds;
        });
    }

    public function testReturnsTheResultWithoutWaitingWhenTheCallSucceeds(): void
    {
        $attempts = 0;
        $result = $this->retrier->call('FinishJob', 42, function () use (&$attempts) {
            $attempts++;

            return ['code' => 200];
        });

        $this->assertSame(['code' => 200], $result);
        $this->assertSame(1, $attempts);
        $this->assertSame([], $this->waits);
        $this->assertSame([], $this->logger->records);
    }

    public function testRetriesACommitLockTimeoutUntilTheCallSucceeds(): void
    {
        $attempts = 0;
        $result = $this->retrier->call('FinishJob', 42, function () use (&$attempts) {
            $attempts++;
            if ($attempts < 3) {
                throw new TimeoutError(self::COMMIT_LOCK_TIMEOUT);
            }

            return ['code' => 200];
        });

        $this->assertSame(['code' => 200], $result);
        $this->assertSame(3, $attempts);
        $this->assertSame([5, 10], $this->waits, 'Waits before each retry, using the first delays in order');
        $this->assertSame([
            [
                'level' => LogLevel::INFO,
                'message' => 'Bedrock did not run FinishJob, retrying it',
                'context' => ['jobID' => 42, 'retry' => 1, 'delay' => 5, 'message' => self::COMMIT_LOCK_TIMEOUT],
            ],
            [
                'level' => LogLevel::INFO,
                'message' => 'Bedrock did not run FinishJob, retrying it',
                'context' => ['jobID' => 42, 'retry' => 2, 'delay' => 10, 'message' => self::COMMIT_LOCK_TIMEOUT],
            ],
        ], $this->logger->records);
    }

    public function testRetriesWhileTheCircuitBreakerIsOpen(): void
    {
        $attempts = 0;
        $result = $this->retrier->call('RetryJob', 42, function () use (&$attempts) {
            $attempts++;
            if ($attempts === 1) {
                throw new CircuitBreakerOpen(self::BREAKER_OPEN);
            }

            return ['code' => 200];
        });

        $this->assertSame(['code' => 200], $result);
        $this->assertSame(2, $attempts);
        $this->assertSame([5], $this->waits);
        $this->assertSame('Bedrock did not run RetryJob, retrying it', $this->logger->records[0]['message']);
    }

    public function testThrowsTheLastErrorWhenBedrockKeepsRejectingTheCall(): void
    {
        $attempts = 0;
        $lastError = null;
        try {
            $this->retrier->call('FailJob', 42, function () use (&$attempts, &$lastError) {
                $attempts++;
                $lastError = $attempts % 2 ? new TimeoutError(self::COMMIT_LOCK_TIMEOUT) : new CircuitBreakerOpen(self::BREAKER_OPEN);
                throw $lastError;
            });
            $this->fail('The call should throw once the retries run out');
        } catch (BedrockError $e) {
            $this->assertSame($lastError, $e, 'The caller gets the error from the final attempt');
        }

        $this->assertSame(count(JobOutcomeRetrier::RETRY_DELAYS) + 1, $attempts, 'Runs once, then once more after each delay');
        $this->assertSame(JobOutcomeRetrier::RETRY_DELAYS, $this->waits);
        $this->assertCount(count(JobOutcomeRetrier::RETRY_DELAYS), $this->logger->records, 'Logs each retry, and nothing for the final failure');
    }

    public function testUsesTheGivenRetryDelays(): void
    {
        $retrier = new JobOutcomeRetrier($this->logger, function (int $seconds): void {
            $this->waits[] = $seconds;
        }, [1, 2]);

        $attempts = 0;
        try {
            $retrier->call('FinishJob', 42, function () use (&$attempts) {
                $attempts++;
                throw new TimeoutError(self::COMMIT_LOCK_TIMEOUT);
            });
            $this->fail('The call should throw once the retries run out');
        } catch (TimeoutError $e) {
        }

        $this->assertSame(3, $attempts);
        $this->assertSame([1, 2], $this->waits);
    }

    public function testRunsOnceWithoutWaitingWhenThereAreNoRetryDelays(): void
    {
        $retrier = new JobOutcomeRetrier($this->logger, function (int $seconds): void {
            $this->waits[] = $seconds;
        }, []);

        $attempts = 0;
        try {
            $retrier->call('FinishJob', 42, function () use (&$attempts) {
                $attempts++;
                throw new TimeoutError(self::COMMIT_LOCK_TIMEOUT);
            });
            $this->fail('The call should throw');
        } catch (TimeoutError $e) {
        }

        $this->assertSame(1, $attempts);
        $this->assertSame([], $this->waits);
    }

    public function testDefaultRetriesOutlastTheDefaultCircuitBreakerCooldown(): void
    {
        // A worker can reach the retrier just as the breaker opens, so the waits together must be longer than the
        // breaker cooldown for at least one retry to run after it closes.
        $this->assertGreaterThan(10, array_sum(JobOutcomeRetrier::RETRY_DELAYS));
    }

    /**
     * Errors that the caller already handles: the client tries every host before it throws a connection failure, a
     * lost response may mean Bedrock ran the call, and the worker manager handles the job errors itself. These must
     * reach the caller unchanged, so the job ends the same way it does without the retrier.
     *
     * @return array<string, array{Throwable}>
     */
    public static function errorsThatAreNotRetried(): array
    {
        return [
            'could not connect to any host' => [new ConnectionFailure('Could not connect to Bedrock hosts or failovers')],
            'response lost after sending' => [new BedrockError('Bedrock response was empty')],
            'job is not running' => [new IllegalAction('Cannot perform `FinishJob` on job 42')],
            'job does not exist' => [new DoesNotExist('Job 42 does not exist')],
            'worker asked for a retry' => [new RetryableException('Try again later', 60)],
            'error outside Bedrock' => [new RuntimeException('Something else broke')],
        ];
    }

    #[DataProvider('errorsThatAreNotRetried')]
    public function testThrowsOtherErrorsWithoutRetrying(Throwable $error): void
    {
        $attempts = 0;
        try {
            $this->retrier->call('FinishJob', 42, function () use (&$attempts, $error) {
                $attempts++;
                throw $error;
            });
            $this->fail('The call should throw');
        } catch (Throwable $e) {
            $this->assertSame($error, $e);
        }

        $this->assertSame(1, $attempts);
        $this->assertSame([], $this->waits);
        $this->assertSame([], $this->logger->records);
    }

    public function testRetriesAFinishJobThatTimedOutWaitingForTheCommitLock(): void
    {
        $sentHeaders = [];
        $client = $this->createMock(Client::class);
        $client->method('getStats')->willReturn(new NullStats());
        $client->expects($this->exactly(2))
            ->method('call')
            ->willReturnCallback(function (string $method, array $headers) use (&$sentHeaders) {
                $sentHeaders[] = [$method, $headers];
                if (count($sentHeaders) === 1) {
                    throw new TimeoutError(self::COMMIT_LOCK_TIMEOUT);
                }

                return ['code' => 200, 'codeLine' => '200 OK', 'headers' => [], 'body' => []];
            });
        $jobs = new Jobs($client);

        $response = $this->retrier->call('FinishJob', 42, fn () => $jobs->finishJob(42, ['step' => 'done'], '{"step":"start"}'));

        $this->assertSame(200, $response['code']);
        $this->assertSame([5], $this->waits);
        $this->assertSame('FinishJob', $sentHeaders[0][0]);
        $this->assertSame(42, $sentHeaders[0][1]['jobID']);
        $this->assertSame(['step' => 'done'], $sentHeaders[0][1]['data']);
        $this->assertSame('{"step":"start"}', $sentHeaders[0][1]['expectedData']);
        $this->assertSame($sentHeaders[0], $sentHeaders[1], 'The retry sends the same command and headers');
    }

    public function testDoesNotRetryAFinishJobThatBedrockRejected(): void
    {
        $client = $this->createMock(Client::class);
        $client->method('getStats')->willReturn(new NullStats());
        $client->expects($this->once())
            ->method('call')
            ->willReturn(['code' => 405, 'codeLine' => '405 Can only finish RUNNING jobs', 'headers' => [], 'body' => []]);
        $jobs = new Jobs($client);

        $this->expectException(IllegalAction::class);
        try {
            $this->retrier->call('FinishJob', 42, fn () => $jobs->finishJob(42));
        } finally {
            $this->assertSame([], $this->waits);
        }
    }
}
