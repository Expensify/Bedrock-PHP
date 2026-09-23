<?php

namespace Expensify\Bedrock\Metrics;

interface MetricsInterface
{
    /**
     * @param int|float             $value Measurement value
     * @param array<string, string> $tags  Measurement tags
     */
    public function send(string $measurement, $value, array $tags = []): void;
}
