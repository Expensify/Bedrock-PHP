<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Aimd;

/**
 * A snapshot of local job timing for a single control loop.
 *
 * The caller reads these numbers from the localJobs database once per loop and hands them to
 * AimdController::decide(), which keeps the control logic free of any database or clock access so
 * it can be unit tested.
 */
final class AimdIntervalStats
{
    public function __construct(
        // Jobs currently running (ended IS NULL).
        public readonly int $numActive,
        // Count and average duration of jobs that finished in the most recent interval.
        public readonly int $lastIntervalCount,
        public readonly float $lastIntervalAverage,
        // Count and average duration of jobs that finished in the interval before that.
        public readonly int $previousIntervalCount,
        public readonly float $previousIntervalAverage,
    ) {
    }
}
