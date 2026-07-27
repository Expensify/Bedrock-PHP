<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests\Aimd;

use Expensify\Bedrock\Aimd\PerTypeIntervalStats;
use PHPUnit\Framework\TestCase;

/**
 * Tests for how raw `GROUP BY jobName` rows are normalized and merged into per-type stats.
 */
final class PerTypeIntervalStatsTest extends TestCase
{
    public function testNormalizesFullNameToBareType(): void
    {
        // Given an active-jobs row with the full env-prefixed name
        // When we build the per-type stats
        $byType = PerTypeIntervalStats::fromGroupedRows([['www-prod/SmartScan', 5]], [], []);

        // Then it is keyed by the bare worker name
        $this->assertArrayHasKey('SmartScan', $byType);
        $this->assertSame(5, $byType['SmartScan']->numActive);
    }

    public function testJobsWithDifferentParamsCollapseToOneType(): void
    {
        // Given the same job type appearing with different query params
        $activeRows = [
            ['www-prod/HandleUberEmployeeManagementEvent?email=a', 2],
            ['www-prod/HandleUberEmployeeManagementEvent?email=b', 3],
        ];

        // When we build the per-type stats
        $byType = PerTypeIntervalStats::fromGroupedRows($activeRows, [], []);

        // Then they roll up into a single type with the counts summed
        $this->assertSame(['HandleUberEmployeeManagementEvent'], array_keys($byType));
        $this->assertSame(5, $byType['HandleUberEmployeeManagementEvent']->numActive);
    }

    public function testAveragesAreCountWeightedWhenNamesMerge(): void
    {
        // Given two stored names that normalize to the same type with different counts and averages
        $lastRows = [
            ['www-prod/X', 10, 2.0],
            ['www-stag/X', 30, 6.0],
        ];

        // When we build the per-type stats
        $byType = PerTypeIntervalStats::fromGroupedRows([], $lastRows, []);

        // Then counts sum and the average is the count-weighted mean: (10*2 + 30*6) / 40 = 5.0
        $this->assertSame(40, $byType['X']->lastIntervalCount);
        $this->assertEqualsWithDelta(5.0, $byType['X']->lastIntervalAverage, 1e-9);
    }

    public function testTypesPresentInOnlySomeWindowsGetZeros(): void
    {
        // Given type A only active, and type B only finished last interval
        $byType = PerTypeIntervalStats::fromGroupedRows(
            [['www-prod/A', 4]],
            [['www-prod/B', 6, 1.5]],
            [],
        );

        // Then both types exist, with zeros for the windows they were absent from
        $this->assertSame(4, $byType['A']->numActive);
        $this->assertSame(0, $byType['A']->lastIntervalCount);
        $this->assertSame(0, $byType['B']->numActive);
        $this->assertSame(6, $byType['B']->lastIntervalCount);
        $this->assertSame(0, $byType['B']->previousIntervalCount);
    }
}
