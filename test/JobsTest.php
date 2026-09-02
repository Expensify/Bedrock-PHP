<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Jobs;
use Expensify\Bedrock\Jobs\ExpectedDataSnapshot;
use Expensify\Bedrock\Stats\NullStats;

final class RecordingClient extends Client
{
    public string $method = '';
    public array $headers = [];

    public function __construct()
    {
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

function expectInvalidSnapshot(array $job): void
{
    try {
        ExpectedDataSnapshot::fromJob($job);
    } catch (UnexpectedValueException $e) {
        return;
    }

    throw new RuntimeException('Invalid expectedDataBase64 did not fail closed');
}

$legacyJob = ['data' => ['value' => 1]];
expectSame(null, ExpectedDataSnapshot::fromJob($legacyJob), 'A legacy job must not have an expected snapshot');

$exactSnapshots = [
    '{}',
    '{"hugeInteger":9007199254740993,"hugeFloat":9007199254740993.0,"nested":{"values":[true,null,{"empty":{}}]}}',
];
foreach ($exactSnapshots as $exactSnapshot) {
    $job = ['expectedDataBase64' => base64_encode($exactSnapshot)];
    expectSame($exactSnapshot, ExpectedDataSnapshot::fromJob($job), 'The snapshot must remain byte-for-byte exact');
}

expectInvalidSnapshot(['expectedDataBase64' => '***']);
expectInvalidSnapshot(['expectedDataBase64' => []]);
expectInvalidSnapshot(['expectedDataBase64' => '']);

$expectedData = '{"hugeFloat":9007199254740993.0,"empty":{},"nested":{"value":1.2300}}';
$client = new RecordingClient();
$jobs = new Jobs($client);
$otherEmptyHeader = [];
$jobs->call('TestExpectedData', [
    'data' => [],
    'expectedData' => $expectedData,
    'expectedWorkerData' => [],
    'otherEmptyHeader' => $otherEmptyHeader,
]);

expectSame($expectedData, $client->headers['expectedData'], 'Jobs::call must not parse or re-encode expectedData');
expectInstanceOf(stdClass::class, $client->headers['data'], 'Jobs::call must encode empty worker data as an object');
expectInstanceOf(stdClass::class, $client->headers['expectedWorkerData'], 'Jobs::call must encode an empty worker baseline as an object');
expectSame($otherEmptyHeader, $client->headers['otherEmptyHeader'], 'Jobs::call must not normalize unrelated empty headers');

$jobs->call('TestEmptyHeaders', [
    'data' => [],
    'expectedData' => [],
    'expectedWorkerData' => [],
]);
expectSame([], $client->headers['expectedData'], 'Jobs::call must not normalize expectedData');
expectInstanceOf(stdClass::class, $client->headers['data'], 'Jobs::call must normalize empty data');
expectInstanceOf(stdClass::class, $client->headers['expectedWorkerData'], 'Jobs::call must normalize an empty worker baseline');

$jobs->finishJob(7, [], $expectedData);
expectSame('FinishJob', $client->method, 'finishJob must call FinishJob');
expectSame($expectedData, $client->headers['expectedData'], 'finishJob must pass expectedData unchanged');
expectNoVersionHeaders($client->headers, 'finishJob');

$jobs->retryJob(7, 0, [], '', '', null, false, $expectedData, []);
expectSame('RetryJob', $client->method, 'retryJob must call RetryJob');
expectSame($expectedData, $client->headers['expectedData'], 'retryJob must pass expectedData unchanged');
expectInstanceOf(stdClass::class, $client->headers['expectedWorkerData'], 'retryJob must pass the decoded worker baseline');
expectNoVersionHeaders($client->headers, 'retryJob');

$jobs->failJob(7, $expectedData);
expectSame('FailJob', $client->method, 'failJob must call FailJob');
expectSame($expectedData, $client->headers['expectedData'], 'failJob must pass expectedData unchanged');
expectNoVersionHeaders($client->headers, 'failJob');

echo "Jobs tests passed\n";
