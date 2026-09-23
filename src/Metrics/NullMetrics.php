<?php

namespace Expensify\Bedrock\Metrics;

class NullMetrics implements MetricsInterface
{
    public function send(string $measurement, $value, array $tags = []): void
    {
    }
}
