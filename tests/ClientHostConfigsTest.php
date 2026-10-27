<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\Client;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;

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
}
