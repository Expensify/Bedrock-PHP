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

    public function testRawResponsesAndAutomaticFallbacks(): void
    {
        $invalidJSON = '{not valid JSON';
        $raw = self::receive($invalidJSON, false);
        $this->assertTrue($raw['code'] === 200);
        $this->assertTrue($raw['headers']['Content-Type'] === 'application/json');
        $this->assertTrue($raw['body'] === $invalidJSON);
        $this->assertTrue(!isset($raw['rawBody']));

        $json = '{"object":{},"array":[],"value":42}';
        $decoded = self::receive($json);
        $this->assertTrue($decoded['body'] === ['object' => [], 'array' => [], 'value' => 42]);
        $this->assertTrue($decoded['rawBody'] === $json);

        $refreshed = self::receive($json, false, ['X-Session-Refresh-Required' => '1']);
        $this->assertTrue($refreshed['body'] === $decoded['body']);
        $this->assertTrue($refreshed['headers']['X-Session-Refresh-Required'] === '1');
        foreach (['processTime', 'upstreamProcessTime'] as $header) {
            $processed = self::receive($json, false, [$header => '1']);
            $this->assertTrue($processed['body'] === $json);
        }
        $error = self::receive($json, false, [], 403);
        $this->assertTrue($error['code'] === 403);
        $this->assertTrue($error['body'] === $decoded['body']);
        $compressedResponse = self::receive(gzencode($json), false, ['Content-Encoding' => 'gzip']);
        $this->assertTrue($compressedResponse['body'] === $json);
        $this->assertTrue(!isset($compressedResponse['headers']['Content-Encoding']));
        $this->assertTrue($compressedResponse['headers']['Content-Length'] === (string) strlen($json));
        $readOnly = self::receive($json, false, ['processTime' => '0', 'upstreamProcessTime' => '0']);
        $this->assertTrue($readOnly['body'] === $json);

        $client = Client::getInstance(['mainHostConfigs' => ['127.0.0.1' => ['port' => 1]]]);
        $this->assertTrue($client->decodeResponse($compressedResponse)['body'] === $decoded['body']);
        $raw = self::receive($json, false);
        $this->assertTrue($raw['body'] === $json);
        $later = $client->decodeResponse($raw);
        $this->assertTrue($later['body'] === $decoded['body']);
        $this->assertTrue($later['code'] === $raw['code'] && $later['headers'] === $raw['headers']);
        $this->assertTrue($client->decodeResponse($later) === $later);
        $scalar = $client->decodeResponse(['headers' => [], 'body' => '"string result"']);
        $this->assertTrue($scalar['body'] === 'string result');
        $this->assertTrue($client->decodeResponse($scalar) === $scalar);

        $compressed = ['headers' => ['Content-Type' => 'application/json', 'Content-Encoding' => 'gzip'], 'body' => gzencode($json)];
        $this->assertTrue($client->decodeResponse($compressed)['body'] === $decoded['body']);
        $text = ['headers' => ['Content-Type' => 'text/plain'], 'body' => 'plain text'];
        $this->assertTrue($client->decodeResponse($text) === $text);

        try {
            $client->decodeResponse(['headers' => [], 'body' => $invalidJSON]);
            throw new RuntimeException('Invalid JSON was accepted by the decoder');
        } catch (BedrockError $error) {
            $this->assertTrue($error->getMessage() === 'Could not parse JSON from bedrock');
        }
    }
}
