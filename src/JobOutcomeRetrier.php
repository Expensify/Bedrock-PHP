<?php

declare(strict_types=1);

namespace Expensify\Bedrock;

use Closure;
use Expensify\Bedrock\Exceptions\CircuitBreakerOpen;
use Expensify\Bedrock\Exceptions\TimeoutError;
use Psr\Log\LoggerInterface;

/**
 * Retries the FinishJob, RetryJob, and FailJob calls that record how a job ended, when Bedrock did not run them.
 *
 * When the jobs cluster is overloaded, a write that waits too long for the commit lock fails with a 555 timeout, and
 * after enough failures the client's circuit breaker rejects requests without sending them. Neither one changes the
 * job, so it stays RUNNING even though its worker is done. These overloads usually clear within seconds, so waiting
 * and trying again records how the job really ended.
 *
 * Only those two errors are retried, because in both cases Bedrock did not commit the call. Any other error is thrown
 * right away, so the caller handles it exactly as it would without this class.
 */
class JobOutcomeRetrier
{
    /**
     * Seconds to wait before each retry. The circuit breaker stays open for 10 seconds by default, so the later
     * retries run after it closes.
     */
    public const RETRY_DELAYS = [5, 10, 15];

    /** @var LoggerInterface */
    private $logger;

    /** @var Closure */
    private $sleep;

    /** @var int[] */
    private $retryDelays;

    /**
     * @param callable|null $sleep       Called with the number of seconds to wait. Defaults to sleep().
     * @param int[]         $retryDelays Seconds to wait before each retry
     */
    public function __construct(LoggerInterface $logger, ?callable $sleep = null, array $retryDelays = self::RETRY_DELAYS)
    {
        $this->logger = $logger;
        $this->sleep = Closure::fromCallable($sleep ?? 'sleep');
        $this->retryDelays = $retryDelays;
    }

    /**
     * Runs $call and returns its result. While Bedrock rejects the call without running it, waits and runs it again.
     *
     * @param string   $command Name of the Bedrock command that $call runs, for logging
     * @param callable $call    Runs the FinishJob, RetryJob, or FailJob call
     *
     * @return mixed What $call returns
     *
     * @throws TimeoutError|CircuitBreakerOpen When Bedrock still rejects the call after the last retry
     */
    public function call(string $command, int $jobID, callable $call)
    {
        foreach ($this->retryDelays as $retry => $delay) {
            try {
                return $call();
            } catch (TimeoutError|CircuitBreakerOpen $e) {
                $this->logger->info("Bedrock did not run $command, retrying it", [
                    'jobID' => $jobID,
                    'retry' => $retry + 1,
                    'delay' => $delay,
                    'message' => $e->getMessage(),
                ]);
                ($this->sleep)($delay);
            }
        }

        return $call();
    }
}
