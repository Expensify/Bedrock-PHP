<?php

declare(strict_types=1);

use Expensify\Bedrock\Client;

require __DIR__.'/../vendor/autoload.php';

if (!function_exists('apcu_enabled') || !apcu_enabled()) {
    throw new RuntimeException('Run with APCu enabled: php -d apc.enable_cli=1 ci/testHostConfigs.php');
}

$assertSame = static function (mixed $expected, mixed $actual, string $message): void {
    if ($expected !== $actual) {
        throw new RuntimeException($message.'; expected '.var_export($expected, true).', got '.var_export($actual, true));
    }
};

$clusterName = 'host-routing-test-'.getmypid();
$cacheKey = Client::APCU_CACHE_PREFIX.$clusterName;
$hosts = [
    'leader' => ['port' => 4444],
    'follower' => ['port' => 4444],
    'other' => ['port' => 4444],
];
$readHosts = array_diff_key($hosts, ['leader' => true]);
$getPossibleHosts = new ReflectionMethod(Client::class, 'getPossibleHosts');
$markHostAsFailed = new ReflectionMethod(Client::class, 'markHostAsFailed');
Client::configure(['writeConsistency' => 'ASYNC', 'maxBlackListTimeout' => 60]);
$writeClient = Client::getInstance([
    'clusterName' => $clusterName,
    'mainHostConfigs' => ['leader' => $hosts['leader']],
    'failoverHostConfigs' => $hosts,
]);
$readClient = Client::getInstance([
    'clusterName' => $clusterName,
    'mainHostConfigs' => ['follower' => $hosts['follower']],
    'failoverHostConfigs' => $readHosts,
]);

try {
    $getPossibleHosts->invoke($writeClient, null);
    $assertSame($hosts, apcu_fetch($cacheKey), 'Initial discovery caches the configured hosts');

    $blacklistedUntil = time() + 3600;
    $cachedHosts = $hosts;
    $cachedHosts['leader']['blacklistedUntil'] = $blacklistedUntil;
    $cachedHosts['follower']['blacklistedUntil'] = $blacklistedUntil;
    apcu_store($cacheKey, $cachedHosts);

    // Alternating routes must not forget failures on either a shared follower or the excluded leader.
    $assertSame(['other' => $hosts['other']], $getPossibleHosts->invoke($readClient, 'leader'), 'Reads exclude blacklisted followers and an explicitly preferred, unconfigured leader');
    $assertSame($cachedHosts, apcu_fetch($cacheKey), 'Read routing preserves the full shared health cache');
    $assertSame(['other' => $hosts['other']], $getPossibleHosts->invoke($writeClient, null), 'Writes still honor the cached failures after a read');
    $assertSame($cachedHosts, apcu_fetch($cacheKey), 'Write routing preserves existing blacklist deadlines');

    $cachedHosts['other']['blacklistedUntil'] = $blacklistedUntil;
    apcu_store($cacheKey, $cachedHosts);
    $assertSame(array_intersect_key($cachedHosts, $readHosts), $getPossibleHosts->invoke($readClient, 'leader'), 'All-blacklisted fallback stays within the read host list');

    $changedPortClient = Client::getInstance([
        'clusterName' => $clusterName,
        'mainHostConfigs' => ['follower' => ['port' => 5555]],
        'failoverHostConfigs' => ['other' => $hosts['other']],
    ]);
    $assertSame(['follower' => ['port' => 5555]], $getPossibleHosts->invoke($changedPortClient, null), 'A changed port resets only that host');
    $cachedHosts['follower'] = ['port' => 5555];
    $assertSame($cachedHosts, apcu_fetch($cacheKey), 'A port change preserves other hosts and their deadlines');

    $newHostClient = Client::getInstance([
        'clusterName' => $clusterName,
        'mainHostConfigs' => ['new-follower' => ['port' => 4444]],
        'failoverHostConfigs' => ['other' => $hosts['other']],
    ]);
    $assertSame(['new-follower' => ['port' => 4444]], $getPossibleHosts->invoke($newHostClient, null), 'New hosts are usable without resetting existing health');
    $cachedHosts['new-follower'] = ['port' => 4444];
    $assertSame($cachedHosts, apcu_fetch($cacheKey), 'Hosts omitted by a client retain their cached state');

    $markHostAsFailed->invoke($readClient, 'other');
    $recordedHosts = apcu_fetch($cacheKey);
    $assertSame($cachedHosts['leader'], $recordedHosts['leader'], "Recording a failure preserves other clients' hosts");
    $assertSame(true, $recordedHosts['other']['blacklistedUntil'] >= time(), 'Recording a failure stores a blacklist deadline');

    // APCu can evict the entry between selecting a host and a connection failure.
    apcu_delete($cacheKey);
    $markHostAsFailed->invoke($changedPortClient, 'follower');
    $recordedHosts = apcu_fetch($cacheKey);
    $assertSame(5555, $recordedHosts['follower']['port'], 'Failure recording restores the actual endpoint after cache eviction');
    $assertSame(true, $recordedHosts['follower']['blacklistedUntil'] >= time(), 'The restored endpoint remains blacklisted');
} finally {
    apcu_delete($cacheKey);
}

echo "Host configuration regression tests passed.\n";
