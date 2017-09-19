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
     * @var int Time (in seconds) to delay the retry.
     */
    private $delay;

    /**
     * @var string DateTime to retry this job
     */
    private $nextRun;

    /**
     * @var string New name for the job
     */
    private $name;

    /**
     * RetryableException constructor.
     *
     * @param string         $message  Message of the exception
     * @param int            $delay    Time (in seconds)  to delay the retry.
     * @param int            $code     Code of the exception
     * @param Exception|null $previous
     * @param string         $nextRun  DateTime to retry the job
     * @param string         $name     New name for the job
     */
    public function __construct($message, $delay = 0, $code = null, Exception $previous = '', $nextRun = '')
    {
        $code = $code ?? 666;
        $this->delay = $delay;
        $this->nextRun = $nextRun;
        $this->name = $name;
        parent::__construct($message, $code, $previous);
    }

    /**
     * Returns the time to delay the retry (in seconds).
     *
     * @return int
     */
    public function getDelay()
    {
        return $this->delay;
    }

    /**
     * Returns the nextRun time.
     *
     * @return string
     */
    public function getNextRun()
    {
        return $this->nextRun;
    }

    /**
     * Returns the new name.
     *
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }
}
