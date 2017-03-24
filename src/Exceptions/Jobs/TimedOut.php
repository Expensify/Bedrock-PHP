<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Exceptions\Jobs;

use Expensify\Bedrock\Exceptions\BedrockError;

/**
 * Thrown when bedrock times out.
 */
class TimedOut extends BedrockError
{
}
