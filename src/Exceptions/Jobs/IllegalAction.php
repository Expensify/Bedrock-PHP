<?php

namespace Expensify\Bedrock\Exceptions\Jobs;

use Expensify\Bedrock\Exceptions\BedrockError;

/**
 * Thrown when trying to perform an illegal action on a job in a 'RUNNING' state.
 */
class IllegalAction extends BedrockError
{
    protected function defineUserFriendlyMessage()
    {
        return "Cannot perform `{$this->data['action']}` on a job in a running state";
    }
}
