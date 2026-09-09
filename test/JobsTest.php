<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Jobs;
use Expensify\Bedrock\Stats\NullStats;

final class RecordingClient extends Client
{
    public string $method = '';
    public array $headers = [];

    public function __construct()
    {
    }

    public function getLogger()
    {
        return new \Psr\Log\NullLogger();
    }

    public function getStats()
    {
        return new NullStats();
    }

    public function call($method, $headers = [], $body = '')
    {
        $this->method = $method;
        $this->headers = $headers;

        return ['code' => 200];
    }
}

function expectSame($expected, $actual, string $message): void
{
    if ($expected !== $actual) {
        throw new RuntimeException($message);
    }
}

function expectInstanceOf(string $class, $actual, string $message): void
{
    if (!$actual instanceof $class) {
        throw new RuntimeException($message);
    }
}

function expectNoVersionHeaders(array $headers, string $method): void
{
    expectSame(false, array_key_exists('enqueueVersion', $headers), "$method must not send enqueueVersion");
    expectSame(false, array_key_exists('dequeueVersion', $headers), "$method must not send dequeueVersion");
}

$expectedData = ['activity' => 1, 'timeoutRetries' => 0];
$client = new RecordingClient();
$jobs = new Jobs($client);
$jobs->createJob('ProcessAgentZeroRequest', ['activity' => 1], null, null, true, rerunIfDataChanged: true);
expectSame(true, $client->headers['rerunIfDataChanged'], 'createJob must send rerunIfDataChanged');
expectSame(false, array_key_exists('uniqueAsRetry', $client->headers), 'createJob must not send the old parameter');

$jobs->createJobs([['name' => 'ProcessAgentZeroRequest', 'unique' => true, 'rerunIfDataChanged' => true]]);
expectSame(true, $client->headers['jobs'][0]['rerunIfDataChanged'], 'createJobs must preserve the renamed field');

$otherEmptyHeader = [];
$jobs->call('TestExpectedData', [
    'data' => [],
    'expectedData' => $expectedData,
    'otherEmptyHeader' => $otherEmptyHeader,
]);

expectSame($expectedData, $client->headers['expectedData'], 'Jobs::call must retain the original data snapshot');
expectInstanceOf(stdClass::class, $client->headers['data'], 'Jobs::call must encode empty worker data as an object');
expectSame($otherEmptyHeader, $client->headers['otherEmptyHeader'], 'Jobs::call must not normalize unrelated empty headers');

$jobs->call('TestEmptyHeaders', [
    'data' => [],
    'expectedData' => [],
]);
expectInstanceOf(stdClass::class, $client->headers['expectedData'], 'Jobs::call must encode an empty snapshot as an object');
expectInstanceOf(stdClass::class, $client->headers['data'], 'Jobs::call must normalize empty data');

$jobs->finishJob(7, [], $expectedData);
expectSame('FinishJob', $client->method, 'finishJob must call FinishJob');
expectSame($expectedData, $client->headers['expectedData'], 'finishJob must pass expectedData unchanged');
expectNoVersionHeaders($client->headers, 'finishJob');

$jobs->retryJob(7, 0, [], '', '', null, false, $expectedData);
expectSame('RetryJob', $client->method, 'retryJob must call RetryJob');
expectSame($expectedData, $client->headers['expectedData'], 'retryJob must pass expectedData unchanged');
expectNoVersionHeaders($client->headers, 'retryJob');

$jobs->failJob(7, $expectedData);
expectSame('FailJob', $client->method, 'failJob must call FailJob');
expectSame($expectedData, $client->headers['expectedData'], 'failJob must pass expectedData unchanged');
expectNoVersionHeaders($client->headers, 'failJob');

echo "Jobs tests passed\n";
