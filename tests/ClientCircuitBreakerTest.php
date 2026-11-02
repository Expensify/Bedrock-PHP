<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\CircuitBreakerOpen;
use Expensify\Bedrock\Exceptions\ConnectionFailure;
use Expensify\Bedrock\JobOutcomeRetrier;
use Expensify\Bedrock\Jobs;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

class ClientCircuitBreakerTest extends TestCase
{
    private array $originalClientState = [];
    private string $clusterName = '';

    protected function setUp(): void
    {
        $this->assertTrue(function_exists('apcu_enabled') && apcu_enabled(), 'Run PHPUnit with the APCu extension and apc.enable_cli=1.');

        foreach (['defaultConfig', 'instances', 'preloadedCommitCounts'] as $propertyName) {
            $property = new ReflectionProperty(Client::class, $propertyName);
            $this->originalClientState[$propertyName] = $property->getValue();
            $property->setValue(null, []);
        }
        Client::configure(['writeConsistency' => 'ASYNC', 'maxBlackListTimeout' => 60]);
        $this->clusterName = 'circuit-breaker-test-'.getmypid();
        $this->deleteClusterState();
    }

    protected function tearDown(): void
    {
        if ($this->clusterName !== '') {
            $this->deleteClusterState();
        }
        foreach ($this->originalClientState as $propertyName => $value) {
            (new ReflectionProperty(Client::class, $propertyName))->setValue(null, $value);
        }
    }

    public function testOpenBreakerThrowsCircuitBreakerOpenWithoutSendingTheRequest(): void
    {
        $client = $this->createClient();
        $this->openBreaker('default');

        try {
            $client->call('FinishJob', ['jobID' => 42, 'idempotent' => true]);
            $this->fail('The call should fail fast while the breaker is open');
        } catch (CircuitBreakerOpen $e) {
            $this->assertInstanceOf(ConnectionFailure::class, $e, 'Callers that catch ConnectionFailure still catch an open breaker');
            $this->assertSame("Bedrock circuit breaker open for cluster $this->clusterName (default)", $e->getMessage());
        }

        $this->assertFalse(apcu_exists(Client::APCU_CACHE_PREFIX.$this->clusterName), 'The client never looked for a host, so it never tried to connect');
    }

    public function testOpenBreakerOnlyAffectsItsOwnScope(): void
    {
        $client = $this->createClient();
        $this->openBreaker('write');

        try {
            $client->call('FinishJob', ['jobID' => 42, 'idempotent' => true]);
            $this->fail('Nothing listens on the test port, so the call should fail to connect');
        } catch (ConnectionFailure $e) {
            $this->assertNotInstanceOf(CircuitBreakerOpen::class, $e, 'The default scope is still closed, so the client tried to connect');
        }
    }

    public function testJobOutcomeRetrierWaitsForAnOpenBreakerToClose(): void
    {
        $client = $this->createClient();
        $jobs = new Jobs($client);
        $this->openBreaker('default');
        $waits = [];
        $retrier = new JobOutcomeRetrier(new RecordingLogger(), function (int $seconds) use (&$waits): void {
            $waits[] = $seconds;

            // The breaker closes when its cooldown ends.
            apcu_delete(Client::CIRCUIT_BREAKER_CACHE_PREFIX."$this->clusterName-default-open");
        });

        try {
            $retrier->call('FinishJob', 42, fn () => $jobs->finishJob(42));
            $this->fail('Nothing listens on the test port, so the retry should fail to connect');
        } catch (ConnectionFailure $e) {
            // After the breaker closes, the retry really tries to connect. The client already tried every host before
            // throwing this, so the retrier hands it to the caller instead of retrying it.
            $this->assertNotInstanceOf(CircuitBreakerOpen::class, $e);
        }

        $this->assertSame([5], $waits, 'Waited once for the breaker, then stopped at the real connection failure');
    }

    private function createClient(): Client
    {
        // Nothing listens on port 1, so any request that gets past the breaker fails to connect right away.
        return Client::getInstance([
            'clusterName' => $this->clusterName,
            'mainHostConfigs' => ['127.0.0.1' => ['port' => 1]],
            'failoverHostConfigs' => [],
        ]);
    }

    private function openBreaker(string $scope): void
    {
        apcu_store(Client::CIRCUIT_BREAKER_CACHE_PREFIX."$this->clusterName-$scope-open", time(), 60);
    }

    private function deleteClusterState(): void
    {
        apcu_delete(Client::APCU_CACHE_PREFIX.$this->clusterName);
        foreach (['default', 'write'] as $scope) {
            foreach (['open', 'fails'] as $suffix) {
                apcu_delete(Client::CIRCUIT_BREAKER_CACHE_PREFIX."$this->clusterName-$scope-$suffix");
            }
        }
    }
}
