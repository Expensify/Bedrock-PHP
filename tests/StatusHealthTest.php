<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\Status;
use PHPUnit\Framework\TestCase;

/**
 * Tests for Status::parseHealth(), which extracts the backend-health fields the AIMD load handler
 * gates on from a Bedrock Status response.
 */
final class StatusHealthTest extends TestCase
{
    public function testParsesAlreadyDecodedBody(): void
    {
        // Given a Status response whose body is already a decoded array
        $response = ['code' => 200, 'body' => [
            'state' => 'LEADING',
            'commandCount' => '42',
            'queuedCommandList' => ['CmdA', 'CmdB', 'CmdC'],
        ]];

        // When we parse it
        $health = Status::parseHealth($response);

        // Then the fields come through, with commandCount coerced to int and the queue depth counted
        $this->assertSame('LEADING', $health['state']);
        $this->assertSame(42, $health['commandCount']);
        $this->assertSame(3, $health['queueDepth']);
    }

    public function testParsesQueuedCommandListWhenItIsAJsonString(): void
    {
        // Given a body where queuedCommandList arrived as a JSON string
        $response = ['body' => ['state' => 'FOLLOWING', 'commandCount' => 5, 'queuedCommandList' => '["a","b"]']];

        // When we parse it
        $health = Status::parseHealth($response);

        // Then the string is decoded and its entries counted
        $this->assertSame(2, $health['queueDepth']);
    }

    public function testParsesRawJsonStringBody(): void
    {
        // Given a body that is still a raw JSON string
        $response = ['body' => '{"state":"LEADING","commandCount":"7","queuedCommandList":[]}'];

        // When we parse it
        $health = Status::parseHealth($response);

        // Then it is decoded and the fields extracted
        $this->assertSame('LEADING', $health['state']);
        $this->assertSame(7, $health['commandCount']);
        $this->assertSame(0, $health['queueDepth']);
    }

    public function testDefaultsWhenFieldsMissing(): void
    {
        // Given a response with no usable body
        // When we parse it
        $health = Status::parseHealth([]);

        // Then we get safe defaults
        $this->assertSame('UNKNOWN', $health['state']);
        $this->assertSame(0, $health['commandCount']);
        $this->assertSame(0, $health['queueDepth']);
    }
}
