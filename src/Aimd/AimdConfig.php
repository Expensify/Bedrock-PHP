<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Aimd;

/**
 * Immutable tuning parameters for the AIMD load handler.
 *
 * These mirror the CLI options parsed in bin/BedrockWorkerManager.php; the defaults here match the
 * defaults declared there so constructing an AimdConfig with no arguments behaves identically to
 * the historical scalar implementation.
 */
final class AimdConfig
{
    public function __construct(
        // The fraction slower the current batch of jobs must be, relative to the previous batch, to
        // cause us to back off the target number of jobs.
        public readonly float $backoffThreshold = 1.1,
        // The fraction of an interval during which a second backoff is suppressed, so we step down
        // once rather than repeatedly on successive loops.
        public readonly float $doubleBackoffPreventionIntervalFraction = 1.0,
        // The length of time (seconds) used to average the speed of recently finished jobs.
        public readonly float $intervalDurationSeconds = 10.0,
        // How many jobs we try to add to the target per second while ramping up.
        public readonly float $jobsToAddPerSecond = 1.0,
        // The floor of the number of jobs we target running simultaneously.
        public readonly int $minSafeJobs = 10,
        // On backoff we multiply the target by this value. Between 0 and 1.
        public readonly float $multiplicativeDecreaseFraction = 0.8,
    ) {
    }
}
