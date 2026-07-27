<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Aimd;

/**
 * A per-type snapshot of local job timing for a single control loop, the per-type analogue of
 * AimdIntervalStats. Instances are keyed by normalized (bare) job type and handed to
 * PerTypeAimdController::decide().
 */
final class PerTypeIntervalStats
{
    public function __construct(
        public readonly int $numActive,
        public readonly int $lastIntervalCount,
        public readonly float $lastIntervalAverage,
        public readonly int $previousIntervalCount,
        public readonly float $previousIntervalAverage,
    ) {
    }

    /**
     * Builds a map of normalized-type => PerTypeIntervalStats from three `GROUP BY jobName` result
     * sets read out of the localJobs DB.
     *
     * Job names are normalized to the bare worker name (e.g. "www-prod/SmartScan" and
     * "www-prod/SmartScan?email=a" both become "SmartScan") so all invocations of a job type roll
     * up together. If two stored names normalize to the same type, their counts are summed and
     * their averages combined as a count-weighted mean.
     *
     * @param array<int, array<int, mixed>> $activeRows rows of [jobName, count]
     * @param array<int, array<int, mixed>> $lastRows   rows of [jobName, count, avg]
     * @param array<int, array<int, mixed>> $prevRows   rows of [jobName, count, avg]
     *
     * @return array<string, self> keyed by normalized job type
     */
    public static function fromGroupedRows(array $activeRows, array $lastRows, array $prevRows): array
    {
        $active = self::indexCounts($activeRows);
        [$lastCount, $lastAvg] = self::indexCountAvg($lastRows);
        [$prevCount, $prevAvg] = self::indexCountAvg($prevRows);

        $types = array_unique(array_merge(array_keys($active), array_keys($lastCount), array_keys($prevCount)));

        $result = [];
        foreach ($types as $type) {
            $result[$type] = new self(
                $active[$type] ?? 0,
                $lastCount[$type] ?? 0,
                $lastAvg[$type] ?? 0.0,
                $prevCount[$type] ?? 0,
                $prevAvg[$type] ?? 0.0,
            );
        }

        return $result;
    }

    /**
     * @param array<int, array<int, mixed>> $rows rows of [jobName, count]
     *
     * @return array<string, int> normalized type => summed count
     */
    private static function indexCounts(array $rows): array
    {
        $counts = [];
        foreach ($rows as $row) {
            $type = self::normalizeName((string) ($row[0] ?? ''));
            $counts[$type] = ($counts[$type] ?? 0) + intval($row[1] ?? 0);
        }

        return $counts;
    }

    /**
     * @param array<int, array<int, mixed>> $rows rows of [jobName, count, avg]
     *
     * @return array{0: array<string, int>, 1: array<string, float>} [summed counts, count-weighted averages]
     */
    private static function indexCountAvg(array $rows): array
    {
        $counts = [];
        $weightedSums = [];
        foreach ($rows as $row) {
            $type = self::normalizeName((string) ($row[0] ?? ''));
            $count = intval($row[1] ?? 0);
            $counts[$type] = ($counts[$type] ?? 0) + $count;
            $weightedSums[$type] = ($weightedSums[$type] ?? 0.0) + floatval($row[2] ?? 0.0) * $count;
        }

        $averages = [];
        foreach ($counts as $type => $count) {
            $averages[$type] = $count > 0 ? $weightedSums[$type] / $count : 0.0;
        }

        return [$counts, $averages];
    }

    /**
     * Reduces a stored job name to its bare worker name: strips any "?params" and the environment
     * prefix, mirroring how BedrockWorkerManager resolves the worker class from a job name.
     */
    private static function normalizeName(string $name): string
    {
        $path = explode('?', $name)[0];
        $segments = explode('/', $path);

        return $segments[1] ?? $path;
    }
}
