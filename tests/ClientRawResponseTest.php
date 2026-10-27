<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\BedrockError;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use RuntimeException;

final class ClientRawResponseTest extends TestCase
{
    private array $originalClientState = [];

    protected function setUp(): void
    {
        foreach (['defaultConfig', 'instances', 'preloadedCommitCounts'] as $name) {
            $property = new ReflectionProperty(Client::class, $name);
            $this->originalClientState[$name] = $property->getValue();
            $property->setValue(null, []);
        }
        Client::configure(['failoverHostConfigs' => []]);
    }

    protected function tearDown(): void
    {
        foreach ($this->originalClientState as $name => $value) {
            (new ReflectionProperty(Client::class, $name))->setValue(null, $value);
        }
    }

    private static function receive(string $body, bool $shouldDecodeResponse = true, array $headers = [], int $code = 200): array
    {
        if (!socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $sockets)) {
            throw new RuntimeException('Could not create the test socket pair');
        }
        $client = Client::getInstance(['clusterName' => 'raw-response-test', 'mainHostConfigs' => ['localhost' => ['port' => 1]]]);
        (new ReflectionProperty(Client::class, 'lastHost'))->setValue($client, 'localhost');
        $socket = new ReflectionProperty(Client::class, 'socket');
        $socket->setValue($client, $sockets[0]);
        try {
            $response = $code." Test Response\r\nContent-Type: application/json\r\n";
            foreach ($headers as $name => $value) {
                $response .= $name.': '.$value."\r\n";
            }
            socket_write($sockets[1], $response.'Content-Length: '.strlen($body)."\r\n\r\n".$body);
            return $client->call('Get', ['idempotent' => false], shouldDecodeResponse: $shouldDecodeResponse);
        } finally {
            $socket->setValue($client, null);
            socket_close($sockets[0]);
            socket_close($sockets[1]);
        }
    }

    public function testAcceptedResponsesCanRemainRaw(): void
    {
        // Given a successful 202 response with no body, when raw mode is requested.
        $empty = self::receive('', false, [], 202);

        // Then preserve the empty body and status without JSON decoding.
        $this->assertSame(202, $empty['code']);
        $this->assertSame('', $empty['body']);
        $this->assertArrayNotHasKey('rawBody', $empty);

        // Given a successful 202 response containing JSON, when raw mode is requested.
        $json = '{"jobID":42}';
        $accepted = self::receive($json, false, [], 202);

        // Then leave JSON untouched while retaining normal behavior for explicit decoding.
        $this->assertSame($json, $accepted['body']);
        $this->assertArrayNotHasKey('rawBody', $accepted);
        $this->assertSame(['jobID' => 42], self::receive($json, true, [], 202)['body']);
        $this->assertSame([], self::receive('', true, [], 202)['body']);

        // When a session refresh is required, then decode even a successful raw-mode response.
        $refreshed = self::receive($json, false, ['X-Session-Refresh-Required' => '1'], 202);
        $this->assertSame(202, $refreshed['code']);
        $this->assertSame(['jobID' => 42], $refreshed['body']);
    }

    public function testRawResponsesAndAutomaticFallbacks(): void
    {
        // Given malformed JSON that the normal decoder would reject.
        $invalidJSON = '{not valid JSON';

        // When raw mode is requested.
        $raw = self::receive($invalidJSON, false);

        // Then return the original body and transport metadata without parsing it.
        $this->assertTrue($raw['code'] === 200);
        $this->assertTrue($raw['headers']['Content-Type'] === 'application/json');
        $this->assertTrue($raw['body'] === $invalidJSON);
        $this->assertTrue(!isset($raw['rawBody']));

        // Given valid JSON, when raw mode is not requested, then retain default decoding.
        $json = '{"object":{},"array":[],"value":42}';
        $decoded = self::receive($json);
        $this->assertTrue($decoded['body'] === ['object' => [], 'array' => [], 'value' => 42]);
        $this->assertTrue($decoded['rawBody'] === $json);

        // When a refresh header is present, then use normal decoding despite raw mode.
        $refreshed = self::receive($json, false, ['X-Session-Refresh-Required' => '1']);
        $this->assertTrue($refreshed['body'] === $decoded['body']);
        $this->assertTrue($refreshed['headers']['X-Session-Refresh-Required'] === '1');

        // When only processing-time headers are present, then keep the body raw.
        foreach (['processTime', 'upstreamProcessTime'] as $header) {
            $processed = self::receive($json, false, [$header => '1']);
            $this->assertTrue($processed['body'] === $json);
        }

        // When an error is returned, then decode its body for normal error handling.
        $error = self::receive($json, false, [], 403);
        $this->assertTrue($error['code'] === 403);
        $this->assertTrue($error['body'] === $decoded['body']);
        $readOnly = self::receive($json, false, ['processTime' => '0', 'upstreamProcessTime' => '0']);
        $this->assertTrue($readOnly['body'] === $json);

        // Given a successful response returned as raw JSON.
        $client = Client::getInstance(['mainHostConfigs' => ['127.0.0.1' => ['port' => 1]]]);
        $raw = self::receive($json, false);
        $this->assertTrue($raw['body'] === $json);

        // When the caller explicitly decodes it.
        $later = $client->decodeResponse($raw);

        // Then preserve metadata and leave subsequent decoding calls unchanged.
        $this->assertTrue($later['body'] === $decoded['body']);
        $this->assertTrue($later['code'] === $raw['code'] && $later['headers'] === $raw['headers']);
        $this->assertTrue($client->decodeResponse($later) === $later);
        $scalar = $client->decodeResponse(['headers' => [], 'body' => '"string result"']);
        $this->assertTrue($scalar['body'] === 'string result');
        $this->assertTrue($client->decodeResponse($scalar) === $scalar);

        $text = ['headers' => ['Content-Type' => 'text/plain'], 'body' => 'plain text'];
        $this->assertTrue($client->decodeResponse($text) === $text);

        // When malformed JSON is explicitly decoded, then reject it.
        try {
            $client->decodeResponse(['headers' => [], 'body' => $invalidJSON]);
            throw new RuntimeException('Invalid JSON was accepted by the decoder');
        } catch (BedrockError $error) {
            $this->assertTrue($error->getMessage() === 'Could not parse JSON from bedrock');
        }
    }
}
