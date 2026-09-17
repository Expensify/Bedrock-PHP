<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\Jobs\DoesNotExist;
use Expensify\Bedrock\Jobs;
use Psr\Log\AbstractLogger;

// Run only against a disposable local Bedrock server. The test creates jobs and starts real worker processes.
$port = (int) getenv('BEDROCK_TEST_PORT');
if (!$port) {
    throw new RuntimeException('Set BEDROCK_TEST_PORT to a disposable local Bedrock server');
}
Client::configure([
    'clusterName' => 'bedrock-php-test',
    'mainHostConfigs' => ['127.0.0.1' => ['port' => $port]],
    'failoverHostConfigs' => [],
    'readTimeout' => 10,
    'bedrockTimeout' => 5,
    'circuitBreakerThreshold' => 0,
]);

if (getenv('BEDROCK_TEST_RUN_MANAGER')) {
    Client::configure(['logger' => new class extends AbstractLogger {
        public function log($level, $message, array $context = [])
        {
            if ($message === 'No worker found, ignoring') {
                // Inject a newer enqueue immediately before the missing-worker FailJob call.
                (new Jobs(Client::getInstance()))->createJob($context['jobName'], ['activity' => 2], unique: true, rerunIfDataChanged: true);
            }
            if ($level === 'alert') {
                fwrite(STDERR, $message."\n");
            }
        }
    }]);
    require dirname(__DIR__).'/bin/BedrockWorkerManager.php';
    exit;
}

function check(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function runManager(string $name, string $directory): void
{
    $process = proc_open([
        PHP_BINARY,
        '-d', 'auto_prepend_file=',
        '-d', 'auto_append_file=',
        __FILE__,
        '--jobName='.$name,
        '--workerPath='.__DIR__.'/fixtures',
        '--maxIterations=1',
        '--maxLoad=10000',
        '--enableLoadHandler',
        '--localJobsDBPath='.$directory.'/local.db',
    ], [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, null, array_merge(getenv(), ['BEDROCK_TEST_RUN_MANAGER' => '1']));
    check(is_resource($process), 'BWM must start');
    $output = stream_get_contents($pipes[1]).stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    check(proc_close($process) === 0, 'BWM must exit normally: '.$output);
    check(!str_contains($output, 'exited abnormally'), 'BWM must process its batch: '.$output);
}

$jobs = new Jobs(Client::getInstance());
$prefix = 'php-snapshot-'.bin2hex(random_bytes(6));
$directory = sys_get_temp_dir().'/'.$prefix;
mkdir($directory);
$jobIDs = [];
$legacy = (bool) getenv('BEDROCK_TEST_LEGACY');
try {
    // Legacy servers normalize omitted data to {}. Preserve it and keep optional headers backward compatible.
    $name = $prefix.'-empty/SnapshotWorker';
    $jobID = (int) $jobs->call('CreateJob', ['name' => $name])['body']['jobID'];
    $jobIDs[] = $jobID;
    $emptyJob = $jobs->getJob($name)['body'];
    check($emptyJob['data'] === [] && $emptyJob['expectedData'] === '{}', 'Empty legacy jobs must preserve {}');
    $jobs->finishJob($jobID, [], $emptyJob['expectedData']);

    // If a snapshot changes {} into [], an opted-in job would spuriously requeue even without another enqueue.
    $name = $prefix.'-unchanged/SnapshotWorker';
    $jobID = (int) $jobs->createJob($name, ['nested' => ['emptyObject' => new stdClass(), 'emptyArray' => [], 'numericKeys' => (object) ['0' => 'zero']]], unique: true, rerunIfDataChanged: !$legacy)['body']['jobID'];
    $jobIDs[] = $jobID;
    runManager($name, $directory);
    try {
        $jobs->queryJob($jobID);
        throw new RuntimeException('Unchanged nested data must finish after one run');
    } catch (DoesNotExist $e) {
        // A completed non-repeating job is deleted.
    }

    if (!$legacy) {
        foreach (['finish', 'retry', 'fail', 'missing'] as $outcome) {
            $name = $prefix.'-'.$outcome.'/'.($outcome === 'missing' ? 'MissingSnapshotWorker' : 'SnapshotWorker');
            $jobID = (int) $jobs->createJob($name, ['activity' => 1, 'outcome' => $outcome, 'workerProgress' => 0], unique: true, rerunIfDataChanged: true)['body']['jobID'];
            $jobIDs[] = $jobID;
            runManager($name, $directory);
            $queued = $jobs->queryJob($jobID);
            check($queued['state'] === Jobs::STATE_QUEUED && $queued['data']['activity'] === 2, $outcome.' must requeue the same job with its newer payload');
            check($queued['data']['workerProgress'] === ($outcome === 'retry' ? 1 : 0), $outcome.' must preserve the correct worker progress');
            if ($outcome === 'missing') {
                $running = $jobs->getJob($name)['body'];
                $jobs->finishJob($jobID, $running['data'], $running['expectedData']);
            } else {
                runManager($name, $directory);
            }
            try {
                $jobs->queryJob($jobID);
                throw new RuntimeException($outcome.' must finish the rerun with its new dequeue snapshot');
            } catch (DoesNotExist $e) {
                // The rerun consumed the newer payload.
            }
        }
    }
    echo 'BWM '.($legacy ? 'legacy compatibility' : 'snapshot integration')." tests passed\n";
} finally {
    foreach ($jobIDs as $jobID) {
        try {
            $jobs->deleteJob($jobID);
        } catch (DoesNotExist $e) {
            // Successfully completed jobs have already been deleted.
        }
    }
    foreach (glob($directory.'/*') as $file) {
        unlink($file);
    }
    rmdir($directory);
}
