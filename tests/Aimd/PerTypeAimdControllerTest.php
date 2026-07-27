<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests\Aimd;

use Expensify\Bedrock\Aimd\PerTypeAimdConfig;
use Expensify\Bedrock\Aimd\PerTypeAimdController;
use Expensify\Bedrock\Aimd\PerTypeIntervalStats;
use PHPUnit\Framework\TestCase;

/**
 * Tests for the per-type AIMD load handler: each job type is paced from its own timing, a type
 * backs off on an absolute latency threshold (not just when accelerating), and critical types keep
 * a guaranteed floor while flooding types are throttled hard.
 */
final class PerTypeAimdControllerTest extends TestCase
{
    private const START = 1000.0;

    private function controller(?PerTypeAimdConfig $config = null): PerTypeAimdController
    {
        return new PerTypeAimdController($config ?? new PerTypeAimdConfig(), self::START);
    }

    private function stats(int $active, int $lastCount, float $lastAvg, int $prevCount, float $prevAvg): PerTypeIntervalStats
    {
        return new PerTypeIntervalStats($active, $lastCount, $lastAvg, $prevCount, $prevAvg);
    }

    /**
     * Run one healthy (fast, steady) tick for each named type with the given active count.
     *
     * @param array<int, string> $types
     */
    private function healthyTick(PerTypeAimdController $c, array $types, int $active, float $now): void
    {
        $map = [];
        foreach ($types as $type) {
            $map[$type] = $this->stats($active, 5, 1.0, 5, 1.0);
        }
        $c->decide($map, $now);
    }

    /**
     * Ramp the named types up over four healthy ticks (START+1 .. START+4).
     *
     * @param array<int, string> $types
     */
    private function rampFourTicks(PerTypeAimdController $c, array $types): void
    {
        for ($i = 1; $i <= 4; $i++) {
            $this->healthyTick($c, $types, 100, self::START + $i);
        }
    }

    public function testColdStartFetchesGlobalBaselineToDiscoverWork(): void
    {
        // Given a fresh controller and an empty local jobs DB (no known types yet)
        $c = $this->controller();

        // When we decide with no per-type stats at all
        $result = $c->decide([], self::START + 1);

        // Then we still fetch the global baseline (minSafeJobs) so new job types get discovered —
        // without this, BWM would never pull anything on a fresh DB.
        $this->assertSame(10, $result);
    }

    public function testInsufficientDataHoldsFloorAndFetchesBaseline(): void
    {
        // Given a fresh controller and a known type with no finished jobs this interval
        $c = $this->controller();

        // When we decide with no jobs active
        $result = $c->decide(['X' => $this->stats(0, 0, 0.0, 0, 0.0)], self::START + 1);

        // Then the type is held at the default floor, and the fetch is lifted to the global
        // baseline (minSafeJobs=10) since the host is otherwise idle
        $this->assertSame(1.0, $c->getTargets()['X']);
        $this->assertSame(10, $result);
    }

    public function testBaselineDoesNotInflateFetchWhenBusy(): void
    {
        // Given a type with lots of active jobs (host is busy, above the baseline)
        $c = $this->controller();

        // When a healthy tick leaves no per-type headroom (target 2 < 100 active)
        $result = $c->decide(['X' => $this->stats(100, 5, 1.0, 5, 1.0)], self::START + 1);

        // Then the baseline floor (minSafeJobs - 100 active) is negative, so it does not add fetch
        $this->assertSame(0, $result);
    }

    public function testHealthyTypeRampsUp(): void
    {
        // Given a fresh controller and plenty of active jobs so the 2x cap doesn't bind
        $c = $this->controller();

        // When one second passes on a healthy tick
        $c->decide(['X' => $this->stats(100, 5, 1.0, 5, 1.0)], self::START + 1);

        // Then the type's target ramps up from the floor by jobsToAddPerSecond
        $this->assertEqualsWithDelta(2.0, $c->getTargets()['X'], 1e-9);
    }

    public function testSlowTypeBacksOffWhileFastTypeKeepsRampingUp(): void
    {
        // Given both a fast and a slow type ramped up to a target of 5
        $c = $this->controller();
        $this->rampFourTicks($c, ['fast', 'slow']);

        // When the slow type exceeds the absolute maxSafeTime (40s > 30s) but the fast one stays quick
        $c->decide([
            'fast' => $this->stats(100, 5, 0.5, 5, 0.5),
            'slow' => $this->stats(100, 5, 40.0, 5, 40.0),
        ], self::START + 5);

        // Then the slow type backs off on its OWN latency while the fast type keeps ramping —
        // exactly the "fast jobs mask slow jobs" case the aggregate handler could not catch.
        $this->assertEqualsWithDelta(6.0, $c->getTargets()['fast'], 1e-9);
        $this->assertEqualsWithDelta(2.5, $c->getTargets()['slow'], 1e-9);
    }

    public function testAbsoluteMaxSafeTimeBacksOffASteadyPlateau(): void
    {
        // Given a type ramped up to 5
        $c = $this->controller();
        $this->rampFourTicks($c, ['Y']);

        // When its latency sits high but steady (40s == 40s, so NOT accelerating)
        $c->decide(['Y' => $this->stats(100, 5, 40.0, 5, 40.0)], self::START + 5);

        // Then it still backs off (target drops, so ramp-up was suppressed) because the absolute
        // threshold fires even when the derivative signal reads "healthy" — the July-8 failure mode.
        $this->assertEqualsWithDelta(2.5, $c->getTargets()['Y'], 1e-9);
    }

    public function testPerTypeOverrideKeepsLegitimatelySlowJobHealthy(): void
    {
        // Given a per-type override that treats SmartScan as fine up to 180s
        $c = $this->controller(new PerTypeAimdConfig(maxSafeTimeOverrides: ['SmartScan' => 180.0]));
        $this->rampFourTicks($c, ['SmartScan']);

        // When SmartScan runs at 40s (over the 30s global default, but under its 180s override)
        $c->decide(['SmartScan' => $this->stats(100, 5, 40.0, 5, 40.0)], self::START + 5);

        // Then it is NOT backed off — it keeps ramping, no false positive
        $this->assertEqualsWithDelta(6.0, $c->getTargets()['SmartScan'], 1e-9);
    }

    public function testCriticalTypeKeepsFloorWhileFloodingTypeCollapses(): void
    {
        // Given a critical type with a guaranteed floor of 10 and a non-critical flooding type
        $c = $this->controller(new PerTypeAimdConfig(criticalTypeFloors: ['SendValidateCode' => 10]));
        $this->rampFourTicks($c, ['SendValidateCode', 'Bad']);

        // When both get slow enough to back off hard
        $c->decide([
            'SendValidateCode' => $this->stats(100, 5, 40.0, 5, 40.0),
            'Bad' => $this->stats(100, 5, 40.0, 5, 40.0),
        ], self::START + 5);

        // Then the critical type is clamped at its floor (14 * 0.5 = 7, floored to 10) while the
        // flooding type is free to collapse toward the low default floor (5 * 0.5 = 2.5)
        $this->assertSame(10.0, $c->getTargets()['SendValidateCode']);
        $this->assertEqualsWithDelta(2.5, $c->getTargets()['Bad'], 1e-9);
    }

    public function testDoubleBackoffPreventedPerType(): void
    {
        // Given a type that just backed off once (5 -> 2.5)
        $c = $this->controller();
        $this->rampFourTicks($c, ['Y']);
        $c->decide(['Y' => $this->stats(100, 5, 40.0, 5, 40.0)], self::START + 5);
        $this->assertEqualsWithDelta(2.5, $c->getTargets()['Y'], 1e-9);

        // When another backoff-worthy tick arrives within the same interval
        $c->decide(['Y' => $this->stats(100, 5, 40.0, 5, 40.0)], self::START + 6);

        // Then it does not back off again
        $this->assertEqualsWithDelta(2.5, $c->getTargets()['Y'], 1e-9);
    }

    public function testAggregateFetchIsSumOfPerTypeHeadroom(): void
    {
        // Given two types each seeded at a floor of 10 with 3 jobs active
        $c = $this->controller(new PerTypeAimdConfig(criticalTypeFloors: ['A' => 10, 'B' => 10]));

        // When we decide with no finished jobs (targets held at the floor)
        $result = $c->decide([
            'A' => $this->stats(3, 0, 0.0, 0, 0.0),
            'B' => $this->stats(3, 0, 0.0, 0, 0.0),
        ], self::START + 1);

        // Then the fetch count is the sum of each type's headroom: (10 - 3) + (10 - 3) = 14
        $this->assertSame(14, $result);
    }

    public function testAggregateFetchClampedToMaxJobsPerFetch(): void
    {
        // Given the same two types but a low per-fetch ceiling of 5
        $c = $this->controller(new PerTypeAimdConfig(criticalTypeFloors: ['A' => 10, 'B' => 10], maxJobsPerFetch: 5));

        // When the summed headroom would be 14
        $result = $c->decide([
            'A' => $this->stats(3, 0, 0.0, 0, 0.0),
            'B' => $this->stats(3, 0, 0.0, 0, 0.0),
        ], self::START + 1);

        // Then it is clamped to the ceiling
        $this->assertSame(5, $result);
    }
}
