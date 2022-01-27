<?php

namespace Expensify\Bedrock\Exceptions\Jobs;

use Exception;
use Expensify\Bedrock\Exceptions\BedrockError;

/**
 * Thrown to signify that a job failed, but it should be retried.
 */
class RetryableException extends BedrockError
{
    /**
     * @var int Time to delay the retry (in seconds)
     */
    private $delay;

    /**
     * @var string When to retry the job (takes precedence over delay; expects format Y-m-d H:i:s)
     */
    private $nextRun;

    /**
     * @var string New name for the job
     */
    private $name;

    /**
     * @var bool When calculating when to retry the job, should we ignore the repeat parameter and use $delay or
     *           $nextRun instead
     */
    private $ignoreRepeat;

    /**
     * RetryableException constructor.
     *
     * @param string     $message      Message of the exception
     * @param int        $delay        Time to delay the retry (in seconds; maximum value is currently 999)
     * @param ?int       $code         Code of the exception
     * @param ?Exception $previous
     * @param string     $name         New name for the job
     * @param string     $nextRun      When to retry the job (takes precedence over delay; expects format Y-m-d H:i:s)
     * @param bool       $ignoreRepeat When calculating when to retry the job, should we ignore the repeat parameter and
     *                                 use $delay or $nextRun instead
     */
    public function __construct(string $message, int $delay = 0, int $code = null, Exception $previous = null, string $name = '', string $nextRun = '', bool $ignoreRepeat = false)
    {
        $code = $code ?? 666;
        $this->delay = $delay;
        $this->nextRun = $nextRun;
        $this->name = $name;
        $this->ignoreRepeat = $ignoreRepeat;
        parent::__construct($message, $code, $previous);
    }

    /**
     * Returns the time to delay the retry (in seconds)
     */
    public function getDelay(): int
    {
        return $this->delay;
    }

    /**
     * Returns the nextRun time
     */
    public function getNextRun(): string
    {
        return $this->nextRun;
    }

    /**
     * Returns the new name
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Returns if we should ignore the jobs repeat parameter when calculating when to retry
     */
    public function getIgnoreRepeat(): bool
    {
        return $this->ignoreRepeat;
    }
}
