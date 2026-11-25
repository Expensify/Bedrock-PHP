<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\Client;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\BackupStaticProperties;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Socket;

#[BackupStaticProperties(true)]
final class ClientRequestTest extends TestCase
{
    private Client $client;
    private Socket $peer;

    protected function setUp(): void
    {
        Client::configure(['failoverHostConfigs' => []]);
        $this->client = Client::getInstance(['clusterName' => 'request-name-test']);
        $this->assertTrue(socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $sockets));
        [$socket, $this->peer] = $sockets;
        (new ReflectionProperty(Client::class, 'lastHost'))->setValue($this->client, 'localhost');
        (new ReflectionProperty(Client::class, 'socket'))->setValue($this->client, $socket);
        socket_write($this->peer, "200 OK\r\nContent-Length: 0\r\n\r\n");
    }

    public static function invalidNames(): iterable
    {
        yield 'blank method' => [' ', []];
        yield 'space in header' => ['Ping', ['test name' => 'value']];
        foreach (['', ':', "\r", "\n", "\r\n\r\n", "\0", "\t", "\x1F", "\x7F"] as $invalid) {
            $name = $invalid === '' ? '' : "test{$invalid}name";
            yield 'method '.bin2hex($invalid) => [$name, []];
            yield 'header '.bin2hex($invalid) => ['Ping', ['valid' => true, $name => 'value']];
        }
    }

    #[DataProvider('invalidNames')]
    public function testInvalidNameWritesNothing(string $method, array $headers): void
    {
        try {
            $this->client->call($method, $headers);
            $this->fail('Expected an invalid request name to be rejected');
        } catch (InvalidArgumentException) {
            $read = [$this->peer];
            $write = $except = null;
            $this->assertSame(0, socket_select($read, $write, $except, 0));
        }
    }

    public function testIndexedHeadersAndBodyKeepTheirFraming(): void
    {
        $method = 'GET /status/handlingCommands HTTP/1.1';
        $this->client->call($method, ['amount[0]' => 15, 'X-Test' => "a:\nb"], 'body');
        $request = socket_read($this->peer, Client::PACKET_LENGTH);
        [$headers, $body] = explode(Client::HEADER_DELIMITER, $request, 2);
        $lines = explode(Client::HEADER_FIELD_SEPARATOR, $headers);
        $this->assertSame($method, $lines[0]);
        $this->assertContains('amount[0]: 15', $lines);
        $this->assertContains('X-Test: a:\nb', $lines);
        $this->assertSame('Content-Length: 4', end($lines));
        $this->assertSame('body', $body);
    }
}
