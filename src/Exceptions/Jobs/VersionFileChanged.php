<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Exceptions\Jobs;

use Expensify\Bedrock\Exceptions\BedrockError;

/**
 * Thrown when the version file changed, meaning we need to stop processing jobs.
 */
class VersionFileChanged extends BedrockError
{
}
