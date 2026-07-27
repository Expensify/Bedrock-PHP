<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Aimd;

/**
 * Immutable tuning parameters for the per-type AIMD load handler.
 *
 * Unlike the scalar handler, this tracks a target per job type. Floors and absolute latency
 * thresholds can be overridden per type: keys are the bare worker name (e.g. "SmartScan"), matching
 * the normalized type key produced by PerTypeIntervalStats. Bedrock-PHP stays generic — the caller
 * (e.g. Salt config) supplies any app-specific overrides.
 */
final class PerTypeAimdConfig
{
    /**
     * @param array<string, int>   $criticalTypeFloors   bare job name => guaranteed minimum target
     * @param array<string, float> $maxSafeTimeOverrides  bare job name => per-type maxSafeTime (seconds)
     */
    public function __construct(
        // Fraction slower this interval must be vs the previous one to back a type off.
        public readonly float $backoffThreshold = 1.1,
        // Fraction of an interval during which a second backoff of the same type is suppressed.
        public readonly float $doubleBackoffPreventionIntervalFraction = 1.0,
        // Length (seconds) of the averaging interval.
        public readonly float $intervalDurationSeconds = 10.0,
        // Jobs added to a type's target per second while ramping up.
        public readonly float $jobsToAddPerSecond = 1.0,
        // Multiplier applied to a type's target on a relative (accelerating) backoff.
        public readonly float $multiplicativeDecreaseFraction = 0.8,
        // Harder multiplier applied when a type exceeds its absolute maxSafeTime.
        public readonly float $absoluteDecreaseFraction = 0.5,
        // Absolute per-job duration (seconds) above which a type is unhealthy regardless of trend.
        // A value <= 0 disables the absolute check.
        public readonly float $maxSafeTime = 30.0,
        // Default per-type floor. Kept low so many job types don't sum to a huge fleet minimum.
        public readonly int $defaultTypeFloor = 1,
        public readonly array $criticalTypeFloors = [],
        public readonly array $maxSafeTimeOverrides = [],
        // Safety ceiling on how many jobs a single GetJobs call may request.
        public readonly int $maxJobsPerFetch = 1000,
    ) {
    }

    /**
     * The floor for a type: its critical override if set, otherwise the default.
     */
    public function floorFor(string $type): int
    {
        return $this->criticalTypeFloors[$type] ?? $this->defaultTypeFloor;
    }

    /**
     * The absolute latency threshold for a type: its override if set, otherwise the default.
     */
    public function maxSafeTimeFor(string $type): float
    {
        return $this->maxSafeTimeOverrides[$type] ?? $this->maxSafeTime;
    }
}
