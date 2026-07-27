<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Aimd;

/**
 * Implemented by both the scalar (AimdController) and per-type (PerTypeAimdController) load
 * handlers so BedrockWorkerManager can report the current target as a stat/log line without caring
 * which implementation is active.
 */
interface AimdTargetReporter
{
    /**
     * The current target job count. For the per-type handler this is the sum across all types.
     */
    public function getTarget(): float;
}
