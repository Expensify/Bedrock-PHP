<?php

namespace Expensify\Bedrock\Exceptions\Jobs;

use Exception;

/**
 * Thrown to signify that a job failed, but it should be retried.
 */
class RetryableException extends Exception
{
    /**
     * @var int Time (in seconds) to delay the retry.
     */
    private $delay;

    /**
     * Bedrock_RetryableException constructor.
     *
     * @param int            $message  Message of the exception
     * @param int            $delay    Time (in seconds)  to delay the retry.
     * @param int            $code     Code of the exception
     * @param Exception|null $previous
     */
    public function __construct($message, $delay = 0, $code = 666, Exception $previous = null)
    {
        $this->delay = $delay;
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
}
