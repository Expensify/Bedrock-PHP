<?php

declare(strict_types=1);

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\Jobs\RetryableException;
use Expensify\Bedrock\Jobs;
use Expensify\Bedrock\LocalDB;

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
 * If the versionWatchFile modified time changes, we stop processing new jobs and exit after finishing all running jobs.
 *
 * Usage: `Usage: sudo -u user php ./bin/BedrockWorkerManager.php --jobName=<jobName> --workerPath=<workerPath> --maxLoad=<maxLoad> [--maxIterations=<iteration> --versionWatchFile=<file> --writeConsistency=<consistency>  --enableLoadHandler --minSafeJobs=<minSafeJobs> --maxSafeTime=<maxSafeTime> --localJobsDBPath=<localJobsDBPath> --debugThrottle]`
 */

// Verify it's being started correctly
if (php_sapi_name() !== "cli") {
    // Throw an exception rather than just output because we assume this is
    // being executed on a webserver, so no STDOUT.  Hopefully they've
    // configured a general uncaught exception handler, and this will trigger
    // that.
    throw new Exception('This script is cli only');
}

// Parse the command line and verify the required settings are provided
$options = getopt('', ['maxLoad::', 'maxIterations::', 'jobName::', 'logger::', 'stats::', 'workerPath::', 'versionWatchFile::', 'writeConsistency::', 'enableLoadHandler', 'minSafeJobs::', 'maxJobsInSingleRun::', 'maxSafeTime::', 'localJobsDBPath::', 'debugThrottle']);

// Store parent ID to determine if we should continue forking
$thisPID = getmypid();

$workerPath = @$options['workerPath'];
if (!$workerPath) {
    echo "Usage: sudo -u user php ./bin/BedrockWorkerManager.php --workerPath=<workerPath> [--jobName=<jobName> --maxLoad=<maxLoad> --maxIterations=<iteration> --writeConsistency=<consistency>  --enableLoadHandler --minSafeJobs=<minSafeJobs> --maxJobsInSingleRun=<maxJobsInSingleRun> --maxSafeTime=<maxSafeTime> --localJobsDBPath=<localJobsDBPath> --debugThrottle]\r\n";
    exit(1);
}

// Add defaults
$jobName = $options['jobName'] ?? '*'; // Process all jobs by default
$maxLoad = floatval($options['maxLoad'] ?? 1.0); // Max load of 1.0 by default
$maxIterations = intval($options['maxIterations'] ?? -1); // Unlimited iterations by default
$pathToDB = $options['localJobsDBPath'] ?? '/tmp/localJobsDB.sql';
$minSafeJobs = intval($options['minSafeJobs'] ?? 10);  // The minimum number of jobs before we start paying attention
$maxJobsForSingleRun = intval($options['maxJobsInSingleRun'] ?? 10);
$maxSafeTime = intval($options['maxSafeTime'] ?? 0); // The maximum job time before we start paying attention
$debugThrottle = isset($options['debugThrottle']); // Set to true to maintain a debug history
$enableLoadHandler = isset($options['enableLoadHandler']); // Enables the AIMD load handler
$target = $minSafeJobs;
$bedrock = Client::getInstance();

// Prepare to use the host logger and stats client, if configured
$logger = $bedrock->getLogger();
$logger->info('Starting BedrockWorkerManager', ['maxIterations' => $maxIterations]);
$stats = $bedrock->getStats();

// Set up the database for the AIMD load handler.
$localDB = new LocalDB($pathToDB, $logger, $stats);
if ($enableLoadHandler) {
    $localDB->open();
    $query = 'CREATE TABLE IF NOT EXISTS localJobs (
        localJobID integer PRIMARY KEY AUTOINCREMENT NOT NULL,
        jobID integer NOT NULL,
        jobName text NOT NULL,
        started text NOT NULL,
        ended text
    );
    CREATE INDEX IF NOT EXISTS localJobsLocalJobID ON localJobs (localJobID);
    PRAGMA journal_mode = WAL;';
    $localDB->write($query);
}

// If --versionWatch is enabled, begin watching a version file for changes
$versionWatchFile = @$options['versionWatchFile'];
$versionWatchFileTimestamp = $versionWatchFile && file_exists($versionWatchFile) ? filemtime($versionWatchFile) : false;

// Wrap everything in a general exception handler so we can handle error
// conditions as gracefully as possible.
try {
    // Validate details now that we have exception handling
    if (!is_dir($workerPath)) {
        throw new Exception("Invalid --workerPath path '$workerPath'");
    }
    if ($maxLoad <= 0) {
        throw new Exception('--maxLoad must be greater than zero');
    }
    $jobs = new Jobs($bedrock);

    // If --maxIterations is set, loop a finite number of times and then self
    // destruct.  This is to guard against memory leaks, as we assume there is
    // some other script that will restart this when it dies.
    $iteration = 0;
    $loopStartTime = 0;
    while (true) {
        // Is it time to self destruct?
        if ($maxIterations > 0 && $iteration >= $maxIterations) {
            $logger->info("We did all our loops iteration, shutting down");
            break;
        }
        $iteration++;
        $logger->info("Loop iteration", ['iteration' => $iteration]);

        $isFirstTry = true;
        // Step One wait for resources to free up
        while (true) {
            $childProcesses = [];
            // Get the latest load
            if (!file_exists('/proc/loadavg')) {
                throw new Exception('are you in a chroot?  If so, please make sure /proc is mounted correctly');
            }

            if ($versionWatchFile && checkVersionFile($versionWatchFile, $versionWatchFileTimestamp, $stats)) {
                $logger->info('Version watch file changed, stop processing new jobs');

                // We break out of this loop and the outer one too. We don't want to process anything more,
                // just wait for child processes to finish.
                break 2;
            }

            // Check if we can fork based on the load of our webservers
            $load = sys_getloadavg()[0];
            list($jobsToQueue, $target) = $stats->benchmark('bedrockerWorkerManager.getNumberOfJobsToQueue', function () use ($localDB, $target, $maxSafeTime, $minSafeJobs, $enableLoadHandler, $maxJobsForSingleRun, $debugThrottle, $logger, $stats) { return getNumberOfJobsToQueue($localDB, $target, $maxSafeTime, $minSafeJobs, $enableLoadHandler, $maxJobsForSingleRun, $debugThrottle, $logger, $stats); });
            if ($load < $maxLoad && $jobsToQueue > 0) {
                $logger->info('Safe to start a new job, checking for more work', ['jobsToQueue' => $jobsToQueue, 'target' => $target, 'load' => $load, 'MAX_LOAD' => $maxLoad]);
                $stats->counter('bedrockWorkerManager.targetJobs', $target);
                break;
            } else {
                $logger->info('Not safe to start a new job, waiting 1s and trying again.', ['jobsToQueue' => $jobsToQueue, 'target' => $target, 'load' => $load, 'MAX_LOAD' => $maxLoad]);
                $isFirstTry = false;
                sleep(1);
            }
        }

        if ($isFirstTry) {
            $stats->timer("bedrockWorkerManager.fullLoop", microtime(true) - $loopStartTime);
        }

        // Poll the server until we successfully get a job
        $response = null;
        while (!$response) {
            if ($versionWatchFile && checkVersionFile($versionWatchFile, $versionWatchFileTimestamp, $stats)) {
                $logger->info('Version watch file changed, stop processing new jobs');

                // We break out of this loop and the outer one too. We don't want to process anything more,
                // just wait for child processes to finish.
                break 2;
            }

            // Ready to get a new job
            try {
                // Query the server for a job
                $response = $jobs->getJobs($jobName, $jobsToQueue);
            } catch (Exception $e) {
                // Try again in 60 seconds
                $logger->info('Problem getting job, retrying in 60s', ['message' => $e->getMessage()]);
                sleep(60);
            }
        }

        $loopStartTime = microtime(true);

        // Found a job
        if ($response['code'] == 200) {
            // BWM jobs are '/' separated names, the last component of which
            // indicates the name of the worker to instantiate to execute this
            // job:
            //
            //     arbitrary/optional/path/to/workerName
            //
            // We look for a file:
            //
            //     <workerPath>/<workerName>.php
            //
            // If it's there, we include it, and then create an object and run
            // it like:
            //
            //     $worker = new $workerName( $job );
            //     $worker->run( );
            //
            // The optional path info allows for jobs to be scheduled
            // selectively.  For example, you may have separate jobs scheduled
            // as production/jobName and staging/jobName, with a WorkerManager
            // in each environment looking for each path.
            $jobsToRun = $response['body']['jobs'];
            foreach ($jobsToRun as $job) {
                $localJobID = 0;
                if ($enableLoadHandler) {
                    $stats->benchmark('bedrockWorkerManager.db.write.insert', function () use ($localDB, $job) { $localDB->write("INSERT INTO localJobs (jobID, jobName, started) VALUES ({$job['jobID']}, '{$job['name']}', ".microtime(true).");"); });
                    $localJobID = $localDB->getLastInsertedRowID();
                }
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
                    //
                    // Note: By explicitly ignoring SIGCHLD we tell the kernel to
                    //       "reap" finished child processes automatically, rather
                    //       than creating "zombie" processes.  (We don't care
                    //       about the child's exit code, so we have no use for the
                    //       zombie process.)
                    $logger->info("Forking and running a worker.", [
                        'workerFileName' => $workerFilename,
                    ]);

                    // Close DB connection before forking.
                    if ($enableLoadHandler) {
                        $localDB->close();
                    }
                    pcntl_signal(SIGCHLD, SIG_IGN);
                    $pid = $stats->benchmark('bedrockWorkerManager.fork', function () { return pcntl_fork(); });
                    if ($pid == -1) {
                        // Something went wrong, couldn't fork
                        $errorMessage = pcntl_strerror(pcntl_get_last_error());
                        throw new Exception("Unable to fork because '$errorMessage', aborting.");
                    } elseif ($pid == 0) {
                        // We forked, so we need to make sure the bedrock client opens new sockets inside this for,
                        // instead of reusing the ones created by the parent process. But we also want to make sure we
                        // keep the same commitCount because we need the finishJob call below to run in a server that has
                        // the commit of the GetJobs call above or the job we are trying to finish might be in QUEUED state.
                        $commitCount = Client::getInstance()->commitCount;
                        Client::clearInstancesAfterFork();
                        $bedrock = Client::getInstance();
                        $bedrock->commitCount = $commitCount;
                        $jobs = new Jobs($bedrock);

                        // If we are using a global REQUEST_ID, reset it to indicate this is a new process.
                        if (isset($GLOBALS['REQUEST_ID'])) {
                            // Reset the REQUEST_ID and re-log the line so we see
                            // it when searching for either the parent and child
                            // REQUEST_IDs.
                            $GLOBALS['REQUEST_ID'] = substr(str_replace(['+', '/'], 'x', base64_encode(openssl_random_pseudo_bytes(6))), 0, 6); // random 6 character ID
                        }
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
                        $stats->benchmark('bedrockJob.finish.'.$job['name'], function () use ($workerName, $bedrock, $jobs, $job, $extraParams, $logger, $localDB, $enableLoadHandler, $localJobID, $stats) {
                            $worker = new $workerName($bedrock, $job);

                            // Open the DB connection after the fork in the child process.
                            try {
                                // Run the worker.  If it completes successfully, finish the job.
                                $worker->run();

                                // Success
                                $logger->info("Job completed successfully, exiting.", [
                                    'name' => $job['name'],
                                    'id' => $job['jobID'],
                                    'extraParams' => $extraParams,
                                ]);
                                $jobs->finishJob($job['jobID'], $worker->getData());
                            } catch (RetryableException $e) {
                                // Worker had a recoverable failure; retry again later.
                                $logger->info("Job could not complete, retrying.", [
                                    'name' => $job['name'],
                                    'id' => $job['jobID'],
                                    'extraParams' => $extraParams,
                                ]);
                                $jobs->retryJob((int) $job['jobID'], $e->getDelay(), $worker->getData(), $e->getName(), $e->getNextRun());
                            } catch (Throwable $e) {
                                $logger->alert("Job failed with errors, exiting.", [
                                    'name' => $job['name'],
                                    'id' => $job['jobID'],
                                    'extraParams' => $extraParams,
                                    'exception' => $e,
                                ]);
                                // Worker had a fatal error -- mark as failed.
                                $jobs->failJob($job['jobID']);
                            } finally {
                                if ($enableLoadHandler) {
                                    $localDB->open();
                                    $stats->benchmark('bedrockWorkerManager.db.write.update', function () use ($localDB, $localJobID) { $localDB->write("UPDATE localJobs SET ended=".microtime(true)." WHERE localJobID=$localJobID;"); });
                                    $localDB->close();
                                }
                            }
                        });

                        // The forked worker process is all done.
                        $stats->counter('bedrockJob.finish.'.$job['name']);
                        exit(1);
                    } else {
                        // Reopen the DB connection in the parent thread.
                        if ($enableLoadHandler) {
                            $localDB->open();
                        }

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
                    $jobs->failJob($job['jobID']);
                }
            }
        } elseif ($response['code'] == 303) {
            $logger->info("No job found, retrying.");
        } else {
            $logger->warning("Failed to get job");
        }
    }
} catch (Throwable $e) {
    $message = $e->getMessage();
    $logger->alert('BedrockWorkerManager.php exited abnormally', ['exception' => $e]);
    echo "Error: $message\r\n";
}

// We wait for all children to finish before dying.
$logger->info('Stopping BedrockWorkerManager, waiting for children');
$status = null;
pcntl_wait($status);
$logger->info('Stopped BedrockWorkerManager');

/**
 * Determines whether or not we call GetJob and try to start a new job
 *
 * @param LocalDB                 $localDB
 * @param int                     $target            The current max number of jobs allowed.
 * @param int                     $maxSafeTime       Maximum safe average time for a batch of jobs before it cuts back.
 * @param bool                    $enableLoadHandler
 * @param int                     $minSafeJobs       A number of jobs that will always be safe to run.
 * @param bool                    $debugThrottle     If true, doesn't delete jobs from localDB
 * @param Psr\Log\LoggerInterface $logger
 *
 * @return array First value how many jobs it is safe to queue, second is an updated $target value.
 */
function getNumberOfJobsToQueue(LocalDB $localDB, int $target, int $maxSafeTime, int $minSafeJobs, bool $enableLoadHandler, int $maxJobsForSingleRun, bool $debugThrottle, Psr\Log\LoggerInterface $logger, $stats): array
{
    // Allow for disabling of the load handler.
    if (!$enableLoadHandler) {
        $logger->info("Load handler not enabled");

        return [$maxJobsForSingleRun, $target];
    }

    // Have we hit our target job count?
    $numActive = $stats->benchmark('bedrockWorkerManager.db.read.activeJobs', function () use ($localDB) { return $localDB->read('SELECT COUNT(*) FROM localJobs WHERE ended IS NULL;')[0]; });
    if ($numActive < $target) {
        // Still in a safe zone, don't worry about load
        $logger->info("Safe to start new job", ["numberOfJobsToQueue" => $target - $numActive, "numActive" => $numActive, "target" => $target]);

        return [$target - $numActive, $target];
    }

    // We're at or over our target; do we have enough data to evaluate the speed?
    $numFinished = $stats->benchmark('bedrockWorkerManager.db.read.completeJobs', function () use ($localDB) { return $localDB->read('SELECT COUNT(*) FROM localJobs WHERE ended IS NOT NULL;')[0]; });
    if ($numFinished < $target * 2) {
        // Wait until we finish at least two batches of our target so we can evaluate its speed,
        // before expanding the batch.
        $logger->info("Haven't finished two batches of target, not queuing job", ['numberOfJobsToQueue' => 0, 'numActive' => $numActive, 'target' => $target]);

        return [0, $target];
    }

    // Calculate the speed of the last 2 batches to see if we're speeding up or slowing down
    $oldBatchTimes = $stats->benchmark('bedrockWorkerManager.db.read.oldBatchTimes', function () use ($localDB, $target) { return $localDB->read("SELECT ended - started FROM localJobs WHERE ended IS NOT NULL ORDER BY ended DESC LIMIT $target OFFSET $target;"); });
    $oldBatchAverageTime = array_sum($oldBatchTimes) / count($oldBatchTimes);
    $newBatchTimes = $stats->benchmark('bedrockWorkerManager.db.read.newBatchTimes', function () use ($localDB, $target) { return $localDB->read("SELECT ended - started FROM localJobs WHERE ended IS NOT NULL ORDER BY ended DESC LIMIT $target;"); });
    $newBatchAverageTime = array_sum($newBatchTimes) / count($newBatchTimes);
    if (($newBatchAverageTime < $maxSafeTime || $newBatchAverageTime < 1.1 * $oldBatchAverageTime) && $numActive <= $target) {
        // The new batch is going fast enough that we don't really care, or if we do care,
        // it's going roughly the same speed as the batch before.  This suggests that we
        // haven't hit any serious bottleneck yet, so let's dial it up ever so slightly and see
        // if speeds hold at the new target.
        $target++;
        $logger->info('Increasing target', ['numberOfJobsToQueue' => $target - $numActive, 'newBatchAverageTime' => $newBatchAverageTime, 'oldBatchAverageTime' => $oldBatchAverageTime, 'numActive' => $numActive, 'target' => $target]);

        // Also, blow away any data from more than two batches ago, because we don't
        // look farther back than that and don't want to accumulate data infinitely.  However,
        // this is very useful data to keep while debugging to analyze our throttle behavior.
        if (!$debugThrottle) {
            $stats->benchmark('bedrockWorkerManager.db.write.deleteOldJobs', function () use ($localDB, $target) { $localDB->write("DELETE FROM localJobs WHERE localJobID IN (SELECT localJobID FROM localJobs WHERE ended IS NOT NULL ORDER BY ended DESC LIMIT -1 OFFSET $target * 2);"); });
        }

        // Authorize one more job given that we've just increased the target by one.
        return [$target - $numActive, $target];
    } elseif ($newBatchAverageTime > $maxSafeTime || $newBatchAverageTime < 1.5 * $oldBatchAverageTime) {
        // Things seem to be slowing down, pull our target back a lot
        $target = intval(max(floor($target / 2), $minSafeJobs));
        $logger->info("Jobs are slowing down, halving the target", ['numberOfJobsToQueue' => $target - $numActive, 'newBatchAverageTime' => $newBatchAverageTime, 'oldBatchAverageTime' => $oldBatchAverageTime, 'numActive' => $numActive, 'target' => $target]);
    }

    // Don't authorize BWM to call GetJobs
    $logger->info("Not queueing job, number of running jobs is above the target", ['numberOfJobsToQueue' => $target - $numActive, 'newBatchAverageTime' => $newBatchAverageTime, 'oldBatchAverageTime' => $oldBatchAverageTime, 'numActive' => $numActive, 'target' => $target]);

    return [0, $target];
}

/**
 * Watch a version file that will cause us to automatically shut
 * down if it changes.  This enables triggering a restart if new
 * PHP is deployed.
 *
 * Note: php's filemtime results are cached, so we need to clear
 *       that cache or we'll be getting a stale modified time.
 *
 * @param Expensify\Bedrock\Stats\StatsInterface $stats
 */
function checkVersionFile(string $versionWatchFile, int $versionWatchFileTimestamp, $stats): bool
{
    return $stats->benchmark('bedrockWorkerManager.checkVersionFile', function () use ($versionWatchFile, $versionWatchFileTimestamp) {
        clearstatcache(true, $versionWatchFile);
        $newVersionWatchFileTimestamp = ($versionWatchFile && file_exists($versionWatchFile)) ? filemtime($versionWatchFile) : false;
        $versionChanged = $versionWatchFile && $newVersionWatchFileTimestamp !== $versionWatchFileTimestamp;

        return $versionChanged;
    });
}
