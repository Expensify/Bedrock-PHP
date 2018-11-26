<?php

namespace Expensify\Bedrock\Exceptions\Cache;

use Expensify\Bedrock\Exceptions\BedrockError;

/**
 * Thrown when bedrock can't find the given cache entry.
 */
class NotFound extends BedrockError
{
}
