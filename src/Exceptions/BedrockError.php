<?php

namespace Expensify\Bedrock\Exceptions;

use Exception;

class BedrockError extends Exception
{
    public function __construct($message, $code = 666, ?Exception $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
