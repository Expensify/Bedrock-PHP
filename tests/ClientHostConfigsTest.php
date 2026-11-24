<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\ConnectionFailure;
use Expensify\Bedrock\Exceptions\TimeoutError;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;
use RuntimeException;
use Throwable;

class ClientHostConfigsTest extends TestCase
{
    private array $originalClientState = [];
    private string $clusterName = '';
    private string $cacheKey = '';

    protected function setUp(): void
    {
        $this->assertTrue(function_exists('apcu_enabled') && apcu_enabled(), 'Run PHPUnit with the APCu extension and apc.enable_cli=1.');

        foreach (['defaultConfig', 'instances', 'preloadedCommitCounts'] as $propertyName) {
            $property = new ReflectionProperty(Client::class, $propertyName);
            $this->originalClientState[$propertyName] = $property->getValue();
            $property->setValue(null, []);
        }
        Client::configure(['writeConsistency' => 'ASYNC', 'maxBlackListTimeout' => 60]);
        $this->clusterName = 'host-routing-test-'.getmypid();
        $this->cacheKey = Client::APCU_CACHE_PREFIX.$this->clusterName;
        apcu_delete($this->cacheKey);
    }

    protected function tearDown(): void
    {
        if ($this->cacheKey !== '') {
            apcu_delete($this->cacheKey);
        }
        foreach ($this->originalClientState as $propertyName => $value) {
            (new ReflectionProperty(Client::class, $propertyName))->setValue(null, $value);
        }
    }

    public function testSharedHostHealthAcrossRoutingConfigurations(): void
    {
        $hosts = [
            'leader' => ['port' => 4444],
            'follower' => ['port' => 4444],
            'other' => ['port' => 4444],
        ];
        $readHosts = array_diff_key($hosts, ['leader' => true]);
        $getPossibleHosts = new ReflectionMethod(Client::class, 'getPossibleHosts');
        $markHostAsFailed = new ReflectionMethod(Client::class, 'markHostAsFailed');
        $writeClient = Client::getInstance([
            'clusterName' => $this->clusterName,
            'mainHostConfigs' => ['leader' => $hosts['leader']],
            'failoverHostConfigs' => $hosts,
        ]);
        $readClient = Client::getInstance([
            'clusterName' => $this->clusterName,
            'mainHostConfigs' => ['follower' => $hosts['follower']],
            'failoverHostConfigs' => $readHosts,
        ]);

        $getPossibleHosts->invoke($writeClient, null);
        $this->assertSame($hosts, apcu_fetch($this->cacheKey), 'Initial discovery caches the configured hosts');

        $blacklistedUntil = time() + 3600;
        $cachedHosts = $hosts;
        $cachedHosts['leader']['blacklistedUntil'] = $blacklistedUntil;
        $cachedHosts['follower']['blacklistedUntil'] = $blacklistedUntil;
        apcu_store($this->cacheKey, $cachedHosts);

        // Alternating routes must not forget failures on either a shared follower or the excluded leader.
        $this->assertSame(['other' => $hosts['other']], $getPossibleHosts->invoke($readClient, 'leader'), 'Reads exclude blacklisted followers and an explicitly preferred, unconfigured leader');
        $this->assertSame($cachedHosts, apcu_fetch($this->cacheKey), 'Read routing preserves the full shared health cache');
        $this->assertSame(['other' => $hosts['other']], $getPossibleHosts->invoke($writeClient, null), 'Writes still honor the cached failures after a read');
        $this->assertSame($cachedHosts, apcu_fetch($this->cacheKey), 'Write routing preserves existing blacklist deadlines');

        $cachedHosts['other']['blacklistedUntil'] = $blacklistedUntil;
        apcu_store($this->cacheKey, $cachedHosts);
        $this->assertSame(array_intersect_key($cachedHosts, $readHosts), $getPossibleHosts->invoke($readClient, 'leader'), 'All-blacklisted fallback stays within the read host list');

        $changedPortClient = Client::getInstance([
            'clusterName' => $this->clusterName,
            'mainHostConfigs' => ['follower' => ['port' => 5555]],
            'failoverHostConfigs' => ['other' => $hosts['other']],
        ]);
        $this->assertSame(['follower' => ['port' => 5555]], $getPossibleHosts->invoke($changedPortClient, null), 'A changed port resets only that host');
        $cachedHosts['follower'] = ['port' => 5555];
        $this->assertSame($cachedHosts, apcu_fetch($this->cacheKey), 'A port change preserves other hosts and their deadlines');

        $newHostClient = Client::getInstance([
            'clusterName' => $this->clusterName,
            'mainHostConfigs' => ['new-follower' => ['port' => 4444]],
            'failoverHostConfigs' => ['other' => $hosts['other']],
        ]);
        $this->assertSame(['new-follower' => ['port' => 4444]], $getPossibleHosts->invoke($newHostClient, null), 'New hosts are usable without resetting existing health');
        $cachedHosts['new-follower'] = ['port' => 4444];
        $this->assertSame($cachedHosts, apcu_fetch($this->cacheKey), 'Hosts omitted by a client retain their cached state');

        $markHostAsFailed->invoke($readClient, 'other');
        $recordedHosts = apcu_fetch($this->cacheKey);
        $this->assertSame($cachedHosts['leader'], $recordedHosts['leader'], "Recording a failure preserves other clients' hosts");
        $this->assertGreaterThanOrEqual(time(), $recordedHosts['other']['blacklistedUntil'], 'Recording a failure stores a blacklist deadline');

        // APCu can evict the entry between selecting a host and a connection failure.
        apcu_delete($this->cacheKey);
        $markHostAsFailed->invoke($changedPortClient, 'follower');
        $recordedHosts = apcu_fetch($this->cacheKey);
        $this->assertSame(5555, $recordedHosts['follower']['port'], 'Failure recording restores the actual endpoint after cache eviction');
        $this->assertGreaterThanOrEqual(time(), $recordedHosts['follower']['blacklistedUntil'], 'The restored endpoint remains blacklisted');
    }

    public function testCommandTimeoutsBlacklistTheHostOnlyWhenRequested(): void
    {
        $default = $this->callHostThatTimesOut([]);
        $this->assertInstanceOf(TimeoutError::class, $default['error'], 'A timeout surfaces without trying another host');
        $this->assertArrayNotHasKey('blacklistedUntil', apcu_fetch($this->cacheKey)['slow'], 'A timeout does not blacklist the host by default');

        $requested = $this->callHostThatTimesOut(['shouldBlacklistHostOnTimeout' => true]);
        $this->assertInstanceOf(TimeoutError::class, $requested['error'], 'A non-idempotent command is not resent after a timeout');
        $this->assertGreaterThanOrEqual(time(), apcu_fetch($this->cacheKey)['slow']['blacklistedUntil'], 'The host that timed out is blacklisted when requested');
        $this->assertStringNotContainsString('shouldBlacklistHostOnTimeout', $requested['request'], 'The option is not sent to the server');

        apcu_delete($this->cacheKey);
        $retried = $this->callHostThatTimesOut(['shouldBlacklistHostOnTimeout' => true, 'idempotent' => true]);
        $this->assertInstanceOf(ConnectionFailure::class, $retried['error'], 'An idempotent command is retried on the next host, which refuses the connection here');
        $this->assertGreaterThanOrEqual(time(), apcu_fetch($this->cacheKey)['slow']['blacklistedUntil'], 'The host that timed out stays blacklisted after the retry');
    }

    /**
     * Calls a host whose connected socket answers with a command timeout. The only other host refuses connections.
     *
     * @return array{error: ?Throwable, request: string}
     */
    private function callHostThatTimesOut(array $headers): array
    {
        if (!socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $sockets)) {
            throw new RuntimeException('Could not create the test socket pair');
        }
        $client = Client::getInstance([
            'clusterName' => $this->clusterName,
            'mainHostConfigs' => ['slow' => ['port' => 4444]],
            'failoverHostConfigs' => ['127.0.0.1' => ['port' => 1]],
        ]);
        (new ReflectionProperty(Client::class, 'lastHost'))->setValue($client, 'slow');
        $socket = new ReflectionProperty(Client::class, 'socket');
        $socket->setValue($client, $sockets[0]);
        socket_write($sockets[1], "555 Timeout peeking command\r\nContent-Length: 0\r\n\r\n");
        $error = null;
        try {
            $client->call('Get', $headers);
        } catch (Throwable $e) {
            $error = $e;
        } finally {
            // Blacklisting a host closes its socket, so only a socket the client still holds needs closing here.
            if ($socket->getValue($client) === $sockets[0]) {
                socket_close($sockets[0]);
            }
            $socket->setValue($client, null);
            $request = (string) socket_read($sockets[1], 65536);
            socket_close($sockets[1]);
        }

        return ['error' => $error, 'request' => $request];
    }
}
