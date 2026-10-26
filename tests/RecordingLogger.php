<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Psr\Log\AbstractLogger;

/**
 * Keeps every log line in memory so tests can check what was logged.
 */
class RecordingLogger extends AbstractLogger
{
    /** @var array<int, array{level: mixed, message: string, context: array}> */
    public array $records = [];

    public function log($level, $message, array $context = [])
    {
        $this->records[] = ['level' => $level, 'message' => (string) $message, 'context' => $context];
    }
}
