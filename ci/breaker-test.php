<?php

declare(strict_types=1);

/*
 * Exercises the Bedrock client's circuit breaker against a live APCu segment. There is no automated
 * suite in this repository, so run this by hand in the dev VM:
 *
 *   php -d auto_prepend_file= ci/breaker-test.php
 *
 * The site bootstrap must be disabled, otherwise it autoloads the released copy of Client before this
 * script can load the working copy. Override BREAKER_TEST_AUTOLOAD if the vendor tree lives elsewhere.
 */

$autoload = getenv('BREAKER_TEST_AUTOLOAD') ?: '/git/expensify.com/vendor/autoload.php';
if (!is_readable($autoload)) {
    fwrite(STDERR, "Cannot read $autoload; set BREAKER_TEST_AUTOLOAD.\n");
    exit(1);
}

// The autoloader supplies the interfaces Client implements, then the working copy is loaded explicitly
// so the released vendor copy is never autoloaded in its place.
require $autoload;
require dirname(__DIR__).'/src/Client.php';

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\BedrockError;

if (!apcu_enabled()) {
    fwrite(STDERR, "APCu is not enabled for CLI, cannot test the breaker.\n");
    exit(1);
}

Client::configure([]);

$results = [];
$check = function (string $name, bool $ok) use (&$results): void {
    $results[] = $ok;
    printf(" [%s] %s\n", $ok ? 'PASS' : 'FAIL', $name);
};

$reflect = new ReflectionClass(Client::class);
$check(
    '0. harness is exercising the working copy of Client',
    $reflect->getMethod('call')->getNumberOfParameters() === 4 && $reflect->hasMethod('breakerCount')
);

$invoke = function (Client $client, string $method, ...$args) use ($reflect) {
    $m = $reflect->getMethod($method);
    $m->setAccessible(true);

    return $m->invokeArgs($client, $args);
};

// Port 1 has nothing listening, so every real call fails at connect.
$makeClient = function (array $overrides = []): Client {
    return Client::getInstance(array_merge([
        'clusterName' => 'testcluster',
        'mainHostConfigs' => ['127.0.0.1' => ['blacklistedUntil' => 0, 'port' => 1]],
        'failoverHostConfigs' => [],
        'connectionTimeout' => 0,
        'connectionTimeoutMicroseconds' => 100000,
        'readTimeout' => 1,
        'maxBlackListTimeout' => 0,
        'circuitBreakerThreshold' => 10,
        'circuitBreakerCooldown' => 10,
    ], $overrides));
};

$repeat = function (Client $client, string $method, string $bucket, int $times) use ($invoke): void {
    for ($i = 0; $i < $times; $i++) {
        $invoke($client, $method, $bucket);
    }
};

$allows = function (Client $client, string $bucket) use ($invoke): bool {
    return $invoke($client, 'circuitBreakerAllowsRequest', $bucket) === true;
};

// 1: the threshold, and that a bucket only ever trips itself.
apcu_clear_cache();
$client = $makeClient();
$repeat($client, 'recordCircuitFailure', 'write', 9);
$check('1. 9 consecutive write failures stays closed', $allows($client, 'write'));
$repeat($client, 'recordCircuitFailure', 'write', 1);
$check('1. the 10th failure opens the write bucket', !$allows($client, 'write'));
$check('1. the read bucket is unaffected', $allows($client, 'read'));

// 2: the exact regression behind the 2026-08-19 miss. Under one shared counter these successes
// would have wiped the write count before it ever reached the threshold.
$repeat($client, 'recordCircuitSuccess', 'read', 200);
$check('2. 200 read successes do not clear the write trip', !$allows($client, 'write'));

// 3: a success within a bucket does end that bucket's run.
apcu_clear_cache();
$client = $makeClient();
$repeat($client, 'recordCircuitFailure', 'write', 9);
$repeat($client, 'recordCircuitSuccess', 'write', 1);
$repeat($client, 'recordCircuitFailure', 'write', 9);
$check('3. a write success resets the run, so 9 + 1 + 9 stays closed', $allows($client, 'write'));

// 4: the known limit of counting a run. A bucket failing steadily but never ten times in a row is
// invisible to this breaker; catching that needs a windowed failure rate.
apcu_clear_cache();
$client = $makeClient();
for ($i = 0; $i < 20; $i++) {
    $repeat($client, 'recordCircuitSuccess', 'write', 1);
    $repeat($client, 'recordCircuitFailure', 'write', 9);
}
$check('4. a sustained 90% failure rate with no run of 10 stays closed, as designed', $allows($client, 'write'));

// 5: the disable switch Auth::timeQuery() relies on.
apcu_clear_cache();
$client = $makeClient(['circuitBreakerThreshold' => 0]);
$repeat($client, 'recordCircuitFailure', 'write', 100);
$check('5. threshold 0 disables the breaker entirely', $allows($client, 'write'));

// 6: the open marker expires on its own once the cooldown passes.
apcu_clear_cache();
$client = $makeClient(['circuitBreakerCooldown' => 1]);
$repeat($client, 'recordCircuitFailure', 'cooldown', 10);
$check('6. the bucket is open immediately after tripping', !$allows($client, 'cooldown'));
sleep(2);
$check('6. the bucket closes itself once the cooldown expires', $allows($client, 'cooldown'));

// 7: a zero cooldown would store a marker that never expires, so it is rejected up front.
$rejected = false;
try {
    $makeClient(['circuitBreakerCooldown' => 0]);
} catch (BedrockError $e) {
    $rejected = true;
}
$check('7. a zero cooldown throws at construction', $rejected);

// 8: the same behaviour end to end through call(), with real connect failures.
apcu_clear_cache();
$client = $makeClient();
$rejectedCalls = 0;
$attempted = 0;
$bucketNamed = false;
for ($i = 0; $i < 15; $i++) {
    try {
        $client->call('Ping', [], '', 'write');
    } catch (BedrockError $e) {
        if (str_contains($e->getMessage(), 'circuit breaker open')) {
            $rejectedCalls++;
            $bucketNamed = $bucketNamed || str_contains($e->getMessage(), '(write)');
        } else {
            $attempted++;
        }
    }
}
$check("8. real failures through call() trip the write bucket then fail fast ($attempted attempted, $rejectedCalls rejected)", $rejectedCalls > 0);
$check('8. the fail-fast message names the bucket', $bucketNamed);

$readRejected = false;
try {
    $client->call('Ping', [], '', 'read');
} catch (BedrockError $e) {
    $readRejected = str_contains($e->getMessage(), 'circuit breaker open');
}
$check('8. read calls are still admitted while the write bucket is open', !$readRejected);

$passed = count(array_filter($results));
$total = count($results);
printf("\n%d/%d passed\n", $passed, $total);
exit($passed === $total ? 0 : 1);
