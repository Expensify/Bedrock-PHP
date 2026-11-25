<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\CircuitBreakerOpen;
use Expensify\Bedrock\Exceptions\ConnectionFailure;
use Expensify\Bedrock\Exceptions\Jobs\IllegalAction;
use Expensify\Bedrock\Exceptions\TimeoutError;
use Expensify\Bedrock\Jobs;
use Expensify\Bedrock\Stats\NullStats;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Throwable;

class JobsTest extends TestCase
{
    private const COMMIT_LOCK_TIMEOUT = 'Internal Bedrock command timeout (555 Timeout waiting for commit lock)';
    private const BREAKER_OPEN = 'Bedrock circuit breaker open for cluster webrock (default)';

    /**
     * Builds a Jobs backed by a stubbed client whose logger is $logger. The delays passed to retryWhileOverloaded are
     * zero in these tests, so the real sleep() returns at once.
     */
    private function jobsWithLogger(LoggerInterface $logger): Jobs
    {
        $client = $this->createStub(Client::class);
        $client->method('getLogger')->willReturn($logger);

        return new Jobs($client);
    }

    public function testReturnsTheResultWithoutRetryingWhenTheCallSucceeds(): void
    {
        // Given a call that succeeds on the first try, and a logger that must never be asked to log a retry
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->never())->method('info');
        $jobs = $this->jobsWithLogger($logger);
        $attempts = 0;

        // When the call is run through retryWhileOverloaded
        $result = $jobs->retryWhileOverloaded('FinishJob', 42, function () use (&$attempts) {
            $attempts++;

            return ['code' => 200];
        }, [0, 0]);

        // Then it returns the result after a single attempt
        $this->assertSame(['code' => 200], $result);
        $this->assertSame(1, $attempts);
    }

    /**
     * @return array<string, array{Throwable}>
     */
    public static function errorsThatAreRetried(): array
    {
        return [
            'commit lock timeout' => [new TimeoutError(self::COMMIT_LOCK_TIMEOUT)],
            'circuit breaker open' => [new CircuitBreakerOpen(self::BREAKER_OPEN)],
        ];
    }

    #[DataProvider('errorsThatAreRetried')]
    public function testRetriesWhileBedrockRejectsTheCallWithoutRunningIt(Throwable $error): void
    {
        // Given a call that Bedrock rejects without running it twice, then accepts, logging each retry
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->exactly(2))
            ->method('info')
            ->with('Bedrock did not run RetryJob, retrying it');
        $jobs = $this->jobsWithLogger($logger);
        $attempts = 0;

        // When the call is run through retryWhileOverloaded
        $result = $jobs->retryWhileOverloaded('RetryJob', 42, function () use (&$attempts, $error) {
            $attempts++;
            if ($attempts < 3) {
                throw $error;
            }

            return ['code' => 200];
        }, [0, 0]);

        // Then it retries after each delay and returns the result once the call succeeds
        $this->assertSame(['code' => 200], $result);
        $this->assertSame(3, $attempts);
    }

    public function testThrowsTheLastErrorWhenBedrockKeepsRejectingTheCall(): void
    {
        // Given a call Bedrock keeps rejecting without running it, and three retry delays
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->exactly(3))->method('info');
        $jobs = $this->jobsWithLogger($logger);
        $attempts = 0;
        $lastError = null;

        // When the retries run out
        try {
            $jobs->retryWhileOverloaded('FailJob', 42, function () use (&$attempts, &$lastError) {
                $attempts++;
                $lastError = $attempts % 2 ? new TimeoutError(self::COMMIT_LOCK_TIMEOUT) : new CircuitBreakerOpen(self::BREAKER_OPEN);
                throw $lastError;
            }, [0, 0, 0]);
            $this->fail('The call should throw once the retries run out');
        } catch (Throwable $e) {
            // Then the caller gets the error from the final attempt, after one try plus one per delay
            $this->assertSame($lastError, $e);
        }

        $this->assertSame(4, $attempts);
    }

    /**
     * Errors the caller already handles reach it unchanged, so the job ends the same way it does without the retry. A
     * plain ConnectionFailure means the client already tried every host, and IllegalAction means Bedrock ran and
     * rejected the call.
     *
     * @return array<string, array{Throwable}>
     */
    public static function errorsThatAreNotRetried(): array
    {
        return [
            'could not connect to any host' => [new ConnectionFailure('Could not connect to Bedrock hosts or failovers')],
            'job is not running' => [new IllegalAction('Cannot perform `FinishJob` on job 42')],
        ];
    }

    #[DataProvider('errorsThatAreNotRetried')]
    public function testThrowsOtherErrorsWithoutRetrying(Throwable $error): void
    {
        // Given a call that fails with an error retryWhileOverloaded does not handle
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->never())->method('info');
        $jobs = $this->jobsWithLogger($logger);
        $attempts = 0;

        // When the call is run through retryWhileOverloaded
        try {
            $jobs->retryWhileOverloaded('FinishJob', 42, function () use (&$attempts, $error) {
                $attempts++;
                throw $error;
            }, [0, 0]);
            $this->fail('The call should throw');
        } catch (Throwable $e) {
            // Then the error reaches the caller unchanged, with no retry
            $this->assertSame($error, $e);
        }

        $this->assertSame(1, $attempts);
    }

    public function testRetriesAFinishJobThatTimedOutWaitingForTheCommitLock(): void
    {
        // Given a FinishJob that times out waiting for the commit lock once, then commits
        $sentHeaders = [];
        $client = $this->createMock(Client::class);
        $client->method('getStats')->willReturn(new NullStats());
        $client->method('getLogger')->willReturn(new NullLogger());
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

        // When finishJob is run through retryWhileOverloaded
        $response = $jobs->retryWhileOverloaded('FinishJob', 42, fn () => $jobs->finishJob(42, ['step' => 'done'], '{"step":"start"}'), [0]);

        // Then the retry sends the same command and headers, and the job finishes
        $this->assertSame(200, $response['code']);
        $this->assertSame('FinishJob', $sentHeaders[0][0]);
        $this->assertSame(42, $sentHeaders[0][1]['jobID']);
        $this->assertSame(['step' => 'done'], $sentHeaders[0][1]['data']);
        $this->assertSame('{"step":"start"}', $sentHeaders[0][1]['expectedData']);
        $this->assertSame($sentHeaders[0], $sentHeaders[1], 'The retry sends the same command and headers');
    }

    public function testDoesNotRetryAFinishJobThatBedrockRejected(): void
    {
        // Given a FinishJob that Bedrock runs and rejects because the job is not RUNNING
        $client = $this->createMock(Client::class);
        $client->method('getStats')->willReturn(new NullStats());
        $client->method('getLogger')->willReturn(new NullLogger());
        $client->expects($this->once())
            ->method('call')
            ->willReturn(['code' => 405, 'codeLine' => '405 Can only finish RUNNING jobs', 'headers' => [], 'body' => []]);
        $jobs = new Jobs($client);

        // When finishJob is run through retryWhileOverloaded, Then it throws without retrying
        $this->expectException(IllegalAction::class);
        $jobs->retryWhileOverloaded('FinishJob', 42, fn () => $jobs->finishJob(42), [0]);
    }
}
