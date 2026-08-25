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
    $reflect->getMethod('call')->getNumberOfParameters() === 4
        && $reflect->hasMethod('breakerWindowCounts')
        && $reflect->hasMethod('breakerCount')
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
        'circuitBreakerThreshold' => 20,
        'circuitBreakerFailureRate' => 0.5,
        'circuitBreakerConsecutiveFailures' => 10,
        'circuitBreakerSliceSeconds' => 5,
        'circuitBreakerWindowSlices' => 6,
        'circuitBreakerCooldown' => 10,
    ], $overrides));
};

$repeat = function (Client $client, string $method, string $bucket, int $times) use ($invoke): void {
    for ($i = 0; $i < $times; $i++) {
        $invoke($client, $method, $bucket);
    }
};

// Alternating outcomes keep the consecutive-failure run at 1, so only the rate trigger can fire.
$interleave = function (Client $client, string $bucket, int $successesPerFailure, int $failures) use ($invoke): void {
    for ($i = 0; $i < $failures; $i++) {
        for ($j = 0; $j < $successesPerFailure; $j++) {
            $invoke($client, 'recordCircuitSuccess', $bucket);
        }
        $invoke($client, 'recordCircuitFailure', $bucket);
    }
};

$allows = function (Client $client, string $bucket) use ($invoke): bool {
    return $invoke($client, 'circuitBreakerAllowsRequest', $bucket) === true;
};

$rateOnly = ['circuitBreakerConsecutiveFailures' => 0];

// 1: the rate trigger opens a bucket that is half failing, without any run of failures.
apcu_clear_cache();
$client = $makeClient();
$interleave($client, 'write', 1, 10);
$check('1. 50% failure rate over 20 calls opens the write bucket', !$allows($client, 'write'));
$check('1. the read bucket is unaffected', $allows($client, 'read'));

// 2: the exact regression behind the 2026-08-19 miss.
$repeat($client, 'recordCircuitSuccess', 'read', 200);
$check('2. 200 read successes do not clear the write trip', !$allows($client, 'write'));

// 3: high volume at a low rate must not trip, which is the 2026-08-19 read path.
apcu_clear_cache();
$client = $makeClient();
$interleave($client, 'read', 20, 10);
$check('3. 10 failures against 200 successes (4.8%) stays closed', $allows($client, 'read'));

// 4: the sample floor, with the run trigger out of the way.
apcu_clear_cache();
$client = $makeClient($rateOnly);
$repeat($client, 'recordCircuitFailure', 'write', 19);
$check('4. 19 failures at 100% is under the 20-sample floor, stays closed', $allows($client, 'write'));
$repeat($client, 'recordCircuitFailure', 'write', 1);
$check('4. the 20th failure crosses the floor and opens it', !$allows($client, 'write'));

// 5: the rate boundary itself.
apcu_clear_cache();
$client = $makeClient($rateOnly);
$repeat($client, 'recordCircuitSuccess', 'write', 11);
$repeat($client, 'recordCircuitFailure', 'write', 9);
$check('5. 9 failures in 20 calls (45%) stays closed', $allows($client, 'write'));

apcu_clear_cache();
$client = $makeClient($rateOnly);
$repeat($client, 'recordCircuitSuccess', 'write', 10);
$repeat($client, 'recordCircuitFailure', 'write', 10);
$check('5. 10 failures in 20 calls (50%) opens it', !$allows($client, 'write'));

// 6: the run trigger. This is the pre-existing behaviour that protected the job fleet on 2026-08-19,
// where the window failure rate only reached 7.93% and the rate trigger would never fire.
apcu_clear_cache();
$client = $makeClient();
$repeat($client, 'recordCircuitFailure', 'all', 9);
$check('6. 9 consecutive failures stays closed', $allows($client, 'all'));
$repeat($client, 'recordCircuitFailure', 'all', 1);
$check('6. 10 consecutive failures open it despite being under the sample floor', !$allows($client, 'all'));

// 7: a success ends the run, so a trickle of failures never accumulates into a trip.
apcu_clear_cache();
$client = $makeClient();
$repeat($client, 'recordCircuitFailure', 'all', 9);
$repeat($client, 'recordCircuitSuccess', 'all', 1);
$repeat($client, 'recordCircuitFailure', 'all', 9);
$check('7. a success resets the run, so 9 + 1 + 9 stays closed', $allows($client, 'all'));

// 8: the run trigger is per bucket too.
apcu_clear_cache();
$client = $makeClient();
$repeat($client, 'recordCircuitFailure', 'write', 10);
$check('8. 10 consecutive write failures open write', !$allows($client, 'write'));
$check('8. and leave read closed to nobody', $allows($client, 'read'));

// 9: the run trigger can be switched off on its own.
apcu_clear_cache();
$client = $makeClient($rateOnly);
$repeat($client, 'recordCircuitFailure', 'write', 10);
$check('9. consecutive failures 0 disables the run trigger', $allows($client, 'write'));

// 10: the disable switch Auth::timeQuery() relies on.
apcu_clear_cache();
$client = $makeClient(['circuitBreakerThreshold' => 0]);
$repeat($client, 'recordCircuitFailure', 'write', 100);
$check('10. threshold 0 disables the breaker entirely', $allows($client, 'write'));

// 11: outcomes leave the window by expiring, not by being reset.
apcu_clear_cache();
$client = $makeClient(['circuitBreakerSliceSeconds' => 1, 'circuitBreakerWindowSlices' => 2, 'circuitBreakerThreshold' => 5, 'circuitBreakerConsecutiveFailures' => 0]);
$repeat($client, 'recordCircuitFailure', 'expiry', 4);
sleep(4);
$repeat($client, 'recordCircuitFailure', 'expiry', 1);
$check('11. failures older than the window age out instead of accumulating', $allows($client, 'expiry'));

// 12: the open marker expires on its own once the cooldown passes.
apcu_clear_cache();
$client = $makeClient(['circuitBreakerCooldown' => 1]);
$repeat($client, 'recordCircuitFailure', 'cooldown', 10);
$check('12. the bucket is open immediately after tripping', !$allows($client, 'cooldown'));
sleep(2);
$check('12. the bucket closes itself once the cooldown expires', $allows($client, 'cooldown'));

// 13: bad settings are rejected at construction, not on the request path.
$rejected = 0;
foreach ([
    ['circuitBreakerSliceSeconds' => 0],
    ['circuitBreakerWindowSlices' => 0],
    ['circuitBreakerFailureRate' => 0],
    ['circuitBreakerFailureRate' => 1.5],
    ['circuitBreakerCooldown' => 0],
] as $bad) {
    try {
        $makeClient($bad);
    } catch (BedrockError $e) {
        $rejected++;
    }
}
$check("13. invalid breaker settings throw at construction ($rejected/5)", $rejected === 5);

// 14: the same behaviour end to end through call(), with real connect failures.
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
$check("14. real failures through call() trip the write bucket then fail fast ($attempted attempted, $rejectedCalls rejected)", $rejectedCalls > 0);
$check('14. the fail-fast message names the bucket', $bucketNamed);

$readRejected = false;
try {
    $client->call('Ping', [], '', 'read');
} catch (BedrockError $e) {
    $readRejected = str_contains($e->getMessage(), 'circuit breaker open');
}
$check('14. read calls are still admitted while the write bucket is open', !$readRejected);

$passed = count(array_filter($results));
$total = count($results);
printf("\n%d/%d passed\n", $passed, $total);
exit($passed === $total ? 0 : 1);
