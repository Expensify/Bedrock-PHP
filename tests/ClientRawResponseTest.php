<?php

declare(strict_types=1);

require __DIR__.'/../vendor/autoload.php';

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\BedrockError;

final class ClientRawResponseTest
{
    private static int $assertions = 0;

    private static function check(bool $condition): void
    {
        ++self::$assertions;
        if (!$condition) {
            throw new RuntimeException('Assertion '.self::$assertions.' failed');
        }
    }

    private static function receive(string $body, ?bool $shouldDecodeResponse): array
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        if ($server === false) {
            throw new RuntimeException($errorMessage, $errorCode);
        }
        $address = stream_socket_get_name($server, false);
        $port = (int) substr($address, strrpos($address, ':') + 1);
        $pid = pcntl_fork();
        if ($pid === -1) {
            fclose($server);
            throw new RuntimeException('Could not fork the test server');
        }
        if ($pid === 0) {
            $connection = stream_socket_accept($server, 5);
            if ($connection === false) {
                exit(1);
            }
            while (($line = fgets($connection)) !== false && trim($line) !== '') {
                // Consume the request headers before sending the fixed test response.
            }
            fwrite($connection, "200 OK\r\nContent-Type: application/json\r\nContent-Length: ".strlen($body)."\r\n\r\n".$body);
            fclose($connection);
            fclose($server);
            exit(0);
        }

        fclose($server);
        try {
            $client = Client::getInstance([
                'clusterName' => 'raw-response-test-'.$port,
                'mainHostConfigs' => ['127.0.0.1' => ['port' => $port]],
                'readTimeout' => 5,
            ]);
            if ($shouldDecodeResponse === null) {
                return $client->call('Get', ['idempotent' => false]);
            }
            return $client->call('Get', ['idempotent' => false], shouldDecodeResponse: $shouldDecodeResponse);
        } finally {
            posix_kill($pid, SIGTERM);
            pcntl_waitpid($pid, $status);
        }
    }

    public static function run(): void
    {
        Client::configure(['failoverHostConfigs' => []]);
        $invalidJSON = '{not valid JSON';
        $raw = self::receive($invalidJSON, false);
        self::check($raw['code'] === 200);
        self::check($raw['headers']['Content-Type'] === 'application/json');
        self::check($raw['body'] === $invalidJSON);
        self::check(!isset($raw['rawBody']));

        $json = '{"object":{},"array":[],"value":42}';
        $decoded = self::receive($json, null);
        self::check($decoded['body'] === ['object' => [], 'array' => [], 'value' => 42]);
        self::check($decoded['rawBody'] === $json);

        $client = Client::getInstance(['mainHostConfigs' => ['127.0.0.1' => ['port' => 1]]]);
        $raw = self::receive($json, false);
        self::check($raw['body'] === $json);
        $later = $client->decodeResponse($raw);
        self::check($later['body'] === $decoded['body']);
        self::check($later['code'] === $raw['code'] && $later['headers'] === $raw['headers']);
        self::check($client->decodeResponse($later) === $later);
        $scalar = $client->decodeResponse(['headers' => [], 'body' => '"string result"']);
        self::check($scalar['body'] === 'string result');
        self::check($client->decodeResponse($scalar) === $scalar);

        $compressed = ['headers' => ['Content-Type' => 'application/json', 'Content-Encoding' => 'gzip'], 'body' => gzencode($json)];
        self::check($client->decodeResponse($compressed)['body'] === $decoded['body']);
        $text = ['headers' => ['Content-Type' => 'text/plain'], 'body' => 'plain text'];
        self::check($client->decodeResponse($text) === $text);

        try {
            $client->decodeResponse(['headers' => [], 'body' => $invalidJSON]);
            throw new RuntimeException('Invalid JSON was accepted by the decoder');
        } catch (BedrockError $error) {
            self::check($error->getMessage() === 'Could not parse JSON from bedrock');
        }

        echo 'Passed '.self::$assertions." raw-response assertions.\n";
    }
}

ClientRawResponseTest::run();
