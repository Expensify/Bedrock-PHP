<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Aimd;

use Psr\Log\LoggerInterface;

/**
 * Per-type AIMD load handler for BedrockWorkerManager.
 *
 * Where AimdController keeps a single global target, this keeps a target per job type and runs the
 * AIMD loop independently for each. That fixes the "fast jobs mask slow jobs" failure: a slow job
 * type is measured against its own history, so it backs off even while thousands of fast jobs of
 * other types keep the aggregate looking healthy.
 *
 * Two changes beyond scalar AIMD:
 *  - a type backs off on an ABSOLUTE latency threshold (maxSafeTime), not just when it is
 *    accelerating, and additive increase is suppressed while a type is over that threshold; and
 *  - the floor is per type (low by default, with a higher guaranteed floor for critical types), so
 *    a flooding type can be throttled hard while critical jobs keep flowing.
 *
 * Like AimdController, all database and clock access happens in the caller and is passed in, so the
 * class is pure and unit testable.
 *
 * NOTE: decide() returns the aggregate number of jobs to fetch (the sum of each type's headroom).
 * Because GetJobs returns a mix the caller does not choose, fully enforcing a single type's target
 * also needs a fork-time admission gate; that is a follow-up. Until then, per-type backoff still
 * reduces how much we pull overall when a type is unhealthy.
 */
final class PerTypeAimdController implements AimdTargetReporter
{
    /** @var array<string, float> target job count per type */
    private array $targets = [];

    /** @var array<string, float> last backoff time per type */
    private array $lastBackoffs = [];

    /** The time decide() last ran, used to scale the additive increase. */
    private float $lastRun;

    public function __construct(
        private readonly PerTypeAimdConfig $config,
        float $now,
        private readonly ?LoggerInterface $logger = null,
    ) {
        $this->lastRun = $now;
    }

    /**
     * The per-type targets, exposed for stats/logging and testing.
     *
     * @return array<string, float>
     */
    public function getTargets(): array
    {
        return $this->targets;
    }

    public function getTarget(): float
    {
        return array_sum($this->targets);
    }

    /**
     * Update each type's target from its own timing snapshot and return the total number of jobs it
     * is safe to queue now (summed headroom across types, clamped to a safety ceiling).
     *
     * @param array<string, PerTypeIntervalStats> $statsByType
     */
    public function decide(array $statsByType, float $now): int
    {
        $timeSinceLastRun = $now - $this->lastRun;
        $this->lastRun = $now;

        $jobsToQueue = 0;
        foreach ($statsByType as $type => $stats) {
            $floor = $this->config->floorFor($type);
            if (!isset($this->targets[$type])) {
                $this->targets[$type] = (float) $floor;
            }

            // Not enough data to compare speeds this interval; hold the target and add its headroom.
            if ($stats->lastIntervalCount === 0 || $stats->previousIntervalCount === 0) {
                $jobsToQueue += $this->headroom($type, $stats);
                continue;
            }

            $maxSafeTime = $this->config->maxSafeTimeFor($type);
            $tooSlowAbsolute = $maxSafeTime > 0.0 && $stats->lastIntervalAverage > $maxSafeTime;
            $tooSlowRelative = $stats->lastIntervalAverage > ($stats->previousIntervalAverage * $this->config->backoffThreshold);

            if ($tooSlowAbsolute || $tooSlowRelative) {
                // Multiplicative decrease, unless we backed this type off very recently. Back off
                // harder when it is over the absolute threshold. Additive increase is implicitly
                // suppressed while over maxSafeTime because we never reach the else branch.
                if (($this->lastBackoffs[$type] ?? 0.0) < $now - ($this->config->intervalDurationSeconds * $this->config->doubleBackoffPreventionIntervalFraction)) {
                    $factor = $tooSlowAbsolute
                        ? min($this->config->multiplicativeDecreaseFraction, $this->config->absoluteDecreaseFraction)
                        : $this->config->multiplicativeDecreaseFraction;
                    $this->targets[$type] = max($this->targets[$type] * $factor, (float) $floor);
                    $this->lastBackoffs[$type] = $now;
                    $this->logger?->info('[AIMD] Backing off type target.', [
                        'type' => $type,
                        'target' => $this->targets[$type],
                        'lastIntervalAverage' => $stats->lastIntervalAverage,
                        'reason' => $tooSlowAbsolute ? 'maxSafeTime' : 'accelerating',
                    ]);
                }
            } else {
                // Ramp up, but don't outrun 2x the currently active jobs of this type.
                if (($this->targets[$type] + $timeSinceLastRun * $this->config->jobsToAddPerSecond) < $stats->numActive * 2) {
                    $this->targets[$type] += $timeSinceLastRun * $this->config->jobsToAddPerSecond;
                }
                $this->logger?->info('[AIMD] Congestion Avoidance for type.', ['type' => $type, 'target' => $this->targets[$type]]);
            }

            $jobsToQueue += $this->headroom($type, $stats);
        }

        return min($jobsToQueue, $this->config->maxJobsPerFetch);
    }

    private function headroom(string $type, PerTypeIntervalStats $stats): int
    {
        return intval(max($this->targets[$type] - $stats->numActive, 0));
    }
}
