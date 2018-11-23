<?php

namespace Expensify\Bedrock\Exceptions\Jobs;

use Expensify\Bedrock\Exceptions\BedrockError;

/**
 * Thrown when bedrock can't find the given cache entry.
 */
class DoesNotExist extends BedrockError
{
}
