<?php

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\Jobs\RetryableException;

/*
 * BedrockWorkerManager
 * ====================
 * This script runs from the command line and performs the following basic logic:
 *
 * 1. Wait for resources to free up
 * 2. Wait for a job
 * 3. Spawn a worker for that job
 * 4. Goto 1
 * After N cycle in the loop, we exit
 *
 * Usage: `Usage: sudo -u user php ./bin/BedrockWorkerManager.php --jobName=<jobName> --workerPath=<workerPath> --maxLoad=<maxLoad> [--host=<host> --port=<port> --maxIterations=<loopIteration>]`
 */

// We need to handle signals with callbacks
declare (ticks = 1);

// Let's go!
// Verify it's being started correctly
if (php_sapi_name() !== "cli") {
    throw new Exception('This script is cli only');
}

$options = getopt('', ['host::', 'port::', 'maxLoad::', 'maxIterations::', 'jobName::', 'logger::', 'stats::', 'workerPath::']);
$jobName = isset($options['jobName']) ? $options['jobName'] : null;
$maxLoad = floatval(isset($options['maxLoad']) ? $options['maxLoad'] : 0);
$maxLoopIteration = (int) isset($options['maxIterations']) ? $options['maxIterations'] : 0;
if (!$maxLoopIteration) {
    $maxLoopIteration = 1000;
}
$bedrockConfig = [];
if (isset($options['host'])) {
    $bedrockConfig['host'] = $options['host'];
}
if (isset($options['port'])) {
    $bedrockConfig['port'] = $options['port'];
}
if (isset($options['logger'])) {
    $bedrockConfig['logger'] = $options['logger'];
}
if (isset($options['stats'])) {
    $bedrockConfig['stats'] = $options['stats'];
}
if (isset($options['connectionTimeout'])) {
    $bedrockConfig['connectionTimeout'] = $options['connectionTimeout'];
}
if (isset($options['readTimeout'])) {
    $bedrockConfig['readTimeout'] = $options['readTimeout'];
}
if (isset($options['failoverHost'])) {
    $bedrockConfig['failoverHost'] = $options['failoverHost'];
}
if (isset($options['failoverPort'])) {
    $bedrockConfig['failoverPort'] = $options['failoverPort'];
}
$workerPath = isset($options['workerPath']) ? $options['workerPath'] : null;
if (!$jobName || !$maxLoad || !$workerPath) {
    throw new Exception('Usage: sudo -u user php ./bin/BedrockWorkerManager.php --jobName=<jobName> --workerPath=<workerPath> --maxLoad=<maxLoad> [--host=<host> --port=<port> --maxIterations=<loopIteration>]');
}
if ($maxLoad <= 0) {
    throw new Exception('Maximum load must be greater than zero');
}

Client::configure($bedrockConfig);

$logger = Client::getLogger();
$stats = Client::getStats();

try {
    $logger->info('Number of Loop iteration before dying', ['maxLoopIteration' => $maxLoopIteration]);

    if (!file_exists('/proc/loadavg')) {
        throw new Exception('Are you sure /proc is mounted?');
    }

    $receivedSIGINT = false;
    $ret = pcntl_signal(SIGINT, function ($signo) use (&$receivedSIGINT, $logger) {
        $logger->info('Received SIGINT signal');
        $receivedSIGINT = true;
    });

    // Connect to Bedrock -- it'll reconnect if necessary
    $bedrock = new Client();

    // Begin the infinite loop
    $loopIteration = 0;
    while (true) {
        if ($receivedSIGINT) {
            $logger->info('Received SIGINT, sleeping');
            sleep(5);
            continue;
        }

        if ($loopIteration === $maxLoopIteration) {
            $logger->info("We did all our loops iteration, shutting down");
            exit(0);
        }

        $loopIteration++;
        $logger->info("Loop iteration", ['iteration' => $loopIteration]);

        // Step One wait for resources to free up
        while (true) {
            // Get the latest load
            $load = sys_getloadavg()[0];
            if ($load < $maxLoad) {
                $logger->info('Load is under max, checking for more work.', ['load' => $load, 'MAX_LOAD' => $maxLoad]);
                break;
            } else {
                $logger->info('Load is over max, waiting 1s and trying again.', ['load' => $load, 'MAX_LOAD' => $maxLoad]);
                sleep(1);
            }
        }

        // Get any job managed by the BedrockWorkerManager
        $response = null;
        while (!$response && !$receivedSIGINT) {
            try {
                // Attempt to get a job
                $response = $bedrock->jobs->getJob($jobName, 60 * 1000); // Wait up to 60s
            } catch (Exception $e) {
                // Try again in 60 seconds
                $logger->info('Problem getting job, retrying in 60s', ['message' => $e->getMessage()]);
                sleep(60);
            }
        }

        if (!$response) {
            $logger->info('Got no response from bedrock... Continuing.');
            continue;
        }

        // Found a job
        if ($response['code'] == 200) {
            // BWM jobs are '/' separated names, the last component of which indicates the name of the worker to
            // instantiate to execute this job:
            // arbitrary/optional/path/to/workerName
            // We look for a file:
            //
            //                    <workerPath>/<workerName>.php
            //
            //                If it's there, we include it, and then create
            //                an object and run it like:
            //
            //                    $worker = new $workerName( $job );
            //                    $worker->safeRun( );
            //
            // The optional path info allows for jobs to be scheduled selectively. I.e., you may have separate jobs
            // scheduled as production/jobName and staging/jobName, with a WorkerManager in each environment looking for
            // each path.
            //
            $job = $response['body'];
            $parts = explode('/', $job['name']);
            $jobParts = explode('?', $job['name']);
            $extraParams = count($jobParts) > 1 ? $jobParts[1] : null;
            $job['name'] = $jobParts[0];
            $workerName = $parts[count($parts) - 1];
            $workerName = explode('?', $workerName)[0];
            $workerFilename = $workerPath."/$workerName.php";
            $logger->info("Looking for worker '$workerFilename'");
            if (file_exists($workerFilename)) {
                // The file seems to exist -- fork it so we can run it.
                $logger->info("Forking and running a worker.", [
                    'workerFileName' => $workerFilename,
                ]);
                // Ignore SIGCHLD signal, which should help 'reap' zombie processes, forcing zombies to kill themselves
                // in the event that the parent process dies before the child/zombie)
                pcntl_signal(SIGCHLD, SIG_IGN);
                $pid = pcntl_fork();
                if ($pid == -1) {
                    // Something went wrong, couldn't fork
                    $errorMessage = pcntl_strerror(pcntl_get_last_error());
                    throw new Exception("Unable to fork because '$errorMessage', aborting.");
                } elseif ($pid == 0) {
                    $logger->info("Fork succeeded, child process, running job", [
                        'name' => $job['name'],
                        'id' => $job['jobID'],
                        'extraParams' => $extraParams,
                    ]);

                    $stats->counter('bedrockJob.create.'.$job['name']);

                    // Include the worker now (not in the parent thread) such
                    // that we automatically pick up new versions over the
                    // worker without needing to restart the parent.
                    include_once $workerFilename;
                    $stats->benchmark('bedrockJob.finish.'.$job['name'], function () use ($workerName, $bedrock, $job, $extraParams, $logger) {
                        $worker = new $workerName($bedrock, $job);
                        try {
                            // Run the worker.  If it completes successfully, finish the job.
                            $worker->run();

                            // Success
                            $logger->info("Job completed successfully, exiting.", [
                                'name' => $job['name'],
                                'id' => $job['jobID'],
                                'extraParams' => $extraParams,
                            ]);
                            $bedrock->jobs->finishJob($job['jobID'], $worker->getData());
                        } catch (RetryableException $e) {
                            // Worker had a recoverable failure; retry again later.
                            $logger->info("Job could not complete, retrying.", [
                                'name' => $job['name'],
                                'id' => $job['jobID'],
                                'extraParams' => $extraParams,
                            ]);
                            $bedrock->jobs->retryJob($job['jobID'], $e->getDelay(), $worker->getData());
                        } catch (Exception $e) {
                            $logger->alert("Job failed with errors, exiting.", [
                                'name' => $job['name'],
                                'id' => $job['jobID'],
                                'extraParams' => $extraParams,
                                'exception' => $e,
                            ]);
                            // Worker had a fatal error -- mark as failed.
                            $bedrock->jobs->failJob($job['jobID']);
                        }
                    });

                    $stats->counter('bedrockJob.finish.'.$job['name']);
                    exit(1);
                } else {
                    // Otherwise we are the parent thread -- continue execution
                    $logger->info("Successfully ran job", [
                        'name' => $job['name'],
                        'id' => $job['jobID'],
                        'pid' => $pid,
                    ]);
                }
            } else {
                // No worker for this job
                $logger->warning('No worker found, ignoring', ['jobName' => $job['name']]);
                $bedrock->jobs->failJob($job['jobID']);
            }
        } elseif ($response['code'] == 303) {
            $logger->info("No job found, retrying.");
        } else {
            $logger->warning("Failed to get job", ['codeLine' => $job['codeLine']]);
        }
    }
} catch (Exception $e) {
    $message = $e->getMessage();
    $logger->alert('BedrockWorkerManager.php exited abnormally', ['exception' => $e]);
}
