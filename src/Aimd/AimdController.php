<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Aimd;

use Psr\Log\LoggerInterface;

/**
 * AIMD (Additive Increase / Multiplicative Decrease) controller for BedrockWorkerManager.
 *
 * Holds the running target (the number of jobs we believe we can safely run at once) and adjusts it
 * once per control loop: back off multiplicatively when jobs are getting slower, otherwise ramp up
 * additively. All database access and clock reads happen in the caller and are passed in via
 * AimdIntervalStats and $now, so this class is pure and unit testable.
 *
 * This is a behavior-preserving extraction of the logic that previously lived inline in
 * getNumberOfJobsToQueue() in bin/BedrockWorkerManager.php.
 */
final class AimdController implements AimdTargetReporter
{
    /** The number of jobs we currently believe we can safely run at once. */
    private float $target;

    /** The time decide() last ran, used to scale the additive increase. */
    private float $lastRun;

    /** The time we last backed off, used to suppress a second backoff within one interval. */
    private float $lastBackoff = 0.0;

    public function __construct(
        private readonly AimdConfig $config,
        float $now,
        private readonly ?LoggerInterface $logger = null,
    ) {
        $this->target = $config->minSafeJobs;
        $this->lastRun = $now;
    }

    /**
     * The current target. Exposed so the caller can emit it as a stat/log line.
     */
    public function getTarget(): float
    {
        return $this->target;
    }

    /**
     * Given this loop's job-timing snapshot and the current time, update the target and return how
     * many additional jobs it is safe to queue right now.
     */
    public function decide(AimdIntervalStats $stats, float $now): int
    {
        $timeSinceLastRun = $now - $this->lastRun;
        $this->lastRun = $now;

        // If we don't have enough data (this interval or the previous one had no finished jobs) we
        // can't compare speeds, so hold the target and return the current headroom.
        if ($stats->lastIntervalCount === 0) {
            $this->logger?->info('[AIMD] No jobs finished this interval, returning default value.', ['minSafeJobs' => $this->config->minSafeJobs, 'returnValue' => max($this->target - $stats->numActive, 0)]);

            return $this->jobsToRun($stats);
        }
        if ($stats->previousIntervalCount === 0) {
            $this->logger?->info('[AIMD] No jobs finished previous interval, returning default value.', ['minSafeJobs' => $this->config->minSafeJobs, 'returnValue' => max($this->target - $stats->numActive, 0)]);

            return $this->jobsToRun($stats);
        }

        // If the last interval's average run time exceeds the previous one by too much, back off.
        if ($stats->lastIntervalAverage > ($stats->previousIntervalAverage * $this->config->backoffThreshold)) {
            // Skip the backoff if we did so too recently (within one interval by default), so we
            // step down once rather than to the floor on each successive loop.
            if ($this->lastBackoff < $now - ($this->config->intervalDurationSeconds * $this->config->doubleBackoffPreventionIntervalFraction)) {
                $this->target = max($this->target * $this->config->multiplicativeDecreaseFraction, $this->config->minSafeJobs);
                $this->lastBackoff = $now;
                $this->logger?->info('[AIMD] Backing off jobs target.', [
                    'target' => $this->target,
                    'lastIntervalAverage' => $stats->lastIntervalAverage,
                    'previousIntervalAverage' => $stats->previousIntervalAverage,
                    'backoffThreshold' => $this->config->backoffThreshold,
                ]);
            }
        } else {
            // Otherwise slowly ramp up, but don't increase past 2x the number of currently running
            // jobs.
            if (($this->target + $timeSinceLastRun * $this->config->jobsToAddPerSecond) < $stats->numActive * 2) {
                $this->target += $timeSinceLastRun * $this->config->jobsToAddPerSecond;
            }
            $this->logger?->info('[AIMD] Congestion Avoidance, incrementing target', ['target' => $this->target]);
        }

        $numJobsToRun = $this->jobsToRun($stats);
        $this->logger?->info('[AIMD] Found number of jobs to run.', [
            'numJobsToRun' => $numJobsToRun,
            'target' => $this->target,
            'numActive' => $stats->numActive,
            'lastIntervalAverage' => $stats->lastIntervalAverage,
            'previousIntervalAverage' => $stats->previousIntervalAverage,
            'lastIntervalCount' => $stats->lastIntervalCount,
            'previousIntervalCount' => $stats->previousIntervalCount,
            'timeSinceLastRun' => $timeSinceLastRun,
        ]);

        return $numJobsToRun;
    }

    /**
     * The number of jobs to queue given the current target: the difference between the target and
     * what's already running, floored at zero.
     */
    private function jobsToRun(AimdIntervalStats $stats): int
    {
        return intval(max($this->target - $stats->numActive, 0));
    }
}
