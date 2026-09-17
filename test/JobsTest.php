<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\Jobs\GenericError;
use Expensify\Bedrock\Jobs;
use Expensify\Bedrock\Stats\NullStats;

final class RecordingClient extends Client
{
    public string $method = '';
    public array $headers = [];
    public array $response = ['code' => 200];

    public function __construct()
    {
    }

    public function getLogger()
    {
        return new Psr\Log\NullLogger();
    }

    public function getStats()
    {
        return new NullStats();
    }

    public function call($method, $headers = [], $body = '')
    {
        $this->method = $method;
        $this->headers = $headers;

        return $this->response;
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

function dequeueResponse(string $rawBody, bool $gzip = false): array
{
    $headers = $gzip ? ['Content-Encoding' => 'gzip'] : [];
    $rawBody = $gzip ? gzencode($rawBody) : $rawBody;
    $parseRawBody = new ReflectionMethod(Client::class, 'parseRawBody');

    return [
        'code' => 200,
        'body' => $parseRawBody->invoke(new RecordingClient(), $headers, $rawBody),
        'headers' => $headers,
        'rawBody' => $rawBody,
    ];
}

$expectedData = ['activity' => 1, 'timeoutRetries' => 0];
$client = new RecordingClient();
$jobs = new Jobs($client);
$jobs->createJob('ProcessAgentZeroRequest', ['activity' => 1], null, null, true, rerunIfDataChanged: true);
expectSame(true, $client->headers['rerunIfDataChanged'], 'createJob must send rerunIfDataChanged');

$jobs->createJob('LegacyJob');
expectSame(false, $client->headers['rerunIfDataChanged'], 'createJob must leave reruns disabled by default');

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
expectSame(['jobID', 'data', 'idempotent', 'expectedData'], array_keys($client->headers), 'finishJob must only send supported headers');

$jobs->retryJob(7, 0, [], '', '', null, false, $expectedData);
expectSame('RetryJob', $client->method, 'retryJob must call RetryJob');
expectSame($expectedData, $client->headers['expectedData'], 'retryJob must pass expectedData unchanged');
expectSame(['jobID', 'delay', 'data', 'name', 'nextRun', 'idempotent', 'jobPriority', 'ignoreRepeat', 'expectedData'], array_keys($client->headers), 'retryJob must only send supported headers');

$jobs->failJob(7, $expectedData);
expectSame('FailJob', $client->method, 'failJob must call FailJob');
expectSame($expectedData, $client->headers['expectedData'], 'failJob must pass expectedData unchanged');
expectSame(['jobID', 'idempotent', 'expectedData'], array_keys($client->headers), 'failJob must only send supported headers');

// Use the real Client decoder: worker data stays associative, while snapshots retain the original JSON types.
$dataJSON = <<<'JSON'
{"emptyObject":{},"emptyArray":[],"numericKeys":{"0":"zero","1":"one"},"nested":{"objects":[{},[]]},"integer":1,"float":1.0,"maxInt":9223372036854775807,"minInt":-9223372036854775808,"escaped":"line\nquote\"backslash\\"}
JSON;
foreach ([false, true] as $gzip) {
    $client->response = dequeueResponse('{"jobID":"7","data":'.$dataJSON.'}', $gzip);
    $job = $jobs->getJob('JobWithNestedData')['body'];
    expectSame(json_decode($dataJSON, true), $job['data'], 'getJob must preserve the existing worker data shape');
    expectSame($dataJSON, $job['expectedData'], 'getJob must preserve object, array, number, and string types');

    $client->response = dequeueResponse('{"jobs":[{"jobID":"7","data":'.$dataJSON.'},{"jobID":"8","data":{}}]}', $gzip);
    $batch = $jobs->getJobs('JobWithNestedData', 2)['body']['jobs'];
    expectSame($dataJSON, $batch[0]['expectedData'], 'getJobs must preserve every job snapshot');
    expectSame('{}', $batch[1]['expectedData'], 'getJobs must preserve empty job data as an object');
}

$client->response = dequeueResponse('{}');
expectSame([], $jobs->getJob('ExcludedJob')['body'], 'getJob must preserve an empty response when every selected job is excluded');
$client->response = dequeueResponse('{"jobs":[]}');
expectSame(['jobs' => []], $jobs->getJobs('ExcludedJobs', 2)['body'], 'getJobs must preserve an empty batch');

$snapshot = $job['expectedData'];
$job['data']['nested']['objects'][0]['workerChange'] = true;
$job['data']['numericKeys'][0] = 'worker';
expectSame($dataJSON, $job['expectedData'], 'Nested worker mutations must not change the immutable snapshot');

$client->response = ['code' => 200];
$jobs->finishJob(7, $job['data'], $snapshot);
expectSame($snapshot, $client->headers['expectedData'], 'finishJob must send the JSON snapshot unchanged');
expectSame($job['data'], $client->headers['data'], 'finishJob must send worker changes separately');
$jobs->retryJob(7, data: $job['data'], expectedData: $snapshot);
expectSame($snapshot, $client->headers['expectedData'], 'retryJob must send the JSON snapshot unchanged');
expectSame($job['data'], $client->headers['data'], 'retryJob must send worker changes separately');
$jobs->failJob(7, $snapshot);
expectSame($snapshot, $client->headers['expectedData'], 'failJob must send the JSON snapshot unchanged');

$jobs->finishJob(7);
expectSame(null, $client->headers['expectedData'], 'finishJob must keep snapshots optional');
$jobs->retryJob(7);
expectSame(null, $client->headers['expectedData'], 'retryJob must keep snapshots optional');
$jobs->failJob(7);
expectSame(null, $client->headers['expectedData'], 'failJob must keep snapshots optional');

$client->response = dequeueResponse('{"jobID":"7","data":null}');
try {
    $jobs->getJob('MalformedJob');
    throw new RuntimeException('A job with an invalid snapshot must not be handed to a worker');
} catch (GenericError $e) {
    expectSame('Cannot preserve the original job data snapshot', $e->getMessage(), 'Invalid snapshots must fail before workers run');
}

echo "Jobs tests passed\n";
