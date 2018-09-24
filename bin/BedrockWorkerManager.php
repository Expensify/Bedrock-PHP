<?php

declare(strict_types=1);

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\Jobs\DoesNotExist;
use Expensify\Bedrock\Exceptions\Jobs\IllegalAction;
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
$maxJobsForSingleRun = intval($options['maxJobsInSingleRun'] ?? 10);
$enableLoadHandler = isset($options['enableLoadHandler']); // Enables the AIMD load handler

// The amount slower that jobs from one interval need to be compared to the previous steady-state value in order to
// cause a backoff in the simultaneous jobs target.
$backoffThreshold = floatval($options['backoffThreshold'] ?? 1.25);

// If set, we don't delete old jobs from our history of calculations for the number of jobs to queue.
$debugThrottle = isset($options['debugThrottle']);

// We assume that it's always safe to run up to this many jobs. Could be called `guaranteedSafeJobCount`.
$minSafeJobs = intval($options['minSafeJobs'] ?? 10);

// If job batches take less time than this, we assume that everything is going fine. Could be called `guaranteedSafeBatchTime`.
$maxSafeTime = intval($options['maxSafeTime'] ?? 0); // The maximum job time before we start paying attention

// Internal state variables for determining the number of jobs to run at one time.
// $target is the number of jobs that we think we can safely run at one time. It defaults to the number of jobs we've
// decided is always safe, and is continually adjusted by `getNumberOfJobsToQueue`.
$target = $minSafeJobs;
// $ssthresh is the slow start threshold, a name stolen from TCP. It indicates the target at which we'll stop doing
// fast ramp up in the number of jobs and switch to the congestion avoidance phase of AIMD. It's initially set high and
// adjusted down to match $target when a congestion event occurs.
$ssthreshDefault = 1000000;
$ssthresh = $ssthreshDefault; 
$steadyStateDuration = 0;

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

        // Step One wait for resources to free up
        $isFirstTry = true;
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

            $jobsToQueue = getNumberOfJobsToQueue();
            echo "Jobs to queue: $jobsToQueue\n";
            if ($load >= $maxLoad) {
                $logger->info('[AIMD2] Not safe to start a new job, load is too high, waiting 1s and trying again.', ['load' => $load, 'MAX_LOAD' => $maxLoad]);
                sleep(1);
            } else if ($jobsToQueue > 0) {
                // $logger->info('[AIMD2] Safe to start a new job, checking for more work', ['jobsToQueue' => $jobsToQueue, 'target' => $target, 'load' => $load, 'MAX_LOAD' => $maxLoad]);
                $stats->timer('bedrockWorkerManager.numberOfJobsToQueue', $target);
                $stats->timer('bedrockWorkerManager.targetJobs', $target);
                break;
            } else {
                // $logger->info('[AIMD2] Not safe to start a new job, waiting 1s and trying again.', ['jobsToQueue' => $jobsToQueue, 'target' => $target, 'load' => $load, 'MAX_LOAD' => $maxLoad]);
                //$localDB->write('DELETE FROM localJobs WHERE started<'.(microtime(true) + 60 * 60).' AND ended IS NULL;');
                // TODO: The above line seems to break things.
                $isFirstTry = false;
                sleep(1);
            }
        }

        // Check to see if BWM was able to get jobs on the first attempt. If not, it would add a full second each time it failed, skewing the timer numbers.
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
                $response = $jobs->getJobs($jobName, $jobsToQueue, ['getMockedJobs' => true]);
            } catch (Exception $e) {
                // Try again in 60 seconds
                $logger->info('Problem getting job, retrying in 60s', ['message' => $e->getMessage()]);
                sleep(60);
            }
        }

        // Found a job
        $loopStartTime = microtime(true);
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
                    $safeJobName = SQLite3::escapeString($job['name']);
                    $stats->benchmark('bedrockWorkerManager.db.write.insert', function () use ($localDB, $job, $safeJobName) {
                        $q = "INSERT INTO localJobs (jobID, jobName, started) VALUES ({$job['jobID']}, '{$safeJobName}', ".microtime(true).");";
                        //echo $q . "\n";
                        $localDB->write($q);
                    });
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
                                if ($worker->getParam("mockRequest") !== '1') {
                                    // Run the worker.  If it completes successfully, finish the job.
                                    $worker->run();

                                    // Success
                                    $logger->info("Job completed successfully, exiting.", [
                                        'name' => $job['name'],
                                        'id' => $job['jobID'],
                                        'extraParams' => $extraParams,
                                    ]);
                                } else {
                                    $logger->info("Mock job, not running and marking as finished.", [
                                        'name' => $job['name'],
                                        'id' => $job['jobID'],
                                        'extraParams' => $extraParams,
                                    ]);
                                }

                                try {
                                    $jobs->finishJob($job['jobID'], $worker->getData());
                                } catch (DoesNotExist $e) {
                                    // Job does not exist, but we know it had to exist because we were running it, so
                                    // we assume this is happening because we retried the command in a different server
                                    // after the first server actually ran the command (but we lost the response).
                                    $logger->info('Failed to FinishJob we probably retried the command so it is safe to ignore', ['job' => $job, 'exception' => $e]);
                                } catch (IllegalAction $e) {
                                    // IllegalAction is returned when we try to finish a job not in RUNNING state (child
                                    // jobs are put in FINISHED state when they are finished), which can happen if we
                                    // retried the command in a different server after the first server actually ran the
                                    // command (but we lost the response).
                                    $logger->info('Failed to FinishJob we probably retried the command on a child job so it is safe to ignore', ['job' => $job, 'exception' => $e]);
                                }
                            } catch (RetryableException $e) {
                                // Worker had a recoverable failure; retry again later.
                                $logger->info("Job could not complete, retrying.", [
                                    'name' => $job['name'],
                                    'id' => $job['jobID'],
                                    'extraParams' => $extraParams,
                                ]);
                                try {
                                    $jobs->retryJob((int) $job['jobID'], $e->getDelay(), $worker->getData(), $e->getName(), $e->getNextRun());
                                } catch (IllegalAction $e) {
                                    // IllegalAction is returned when we try to finish a job that's not RUNNING, this
                                    // can happen if we retried the command in a different server
                                    // after the first server actually ran the command (but we lost the response).
                                    $logger->info('Failed to RetryJob we probably retried the command so it is safe to ignore', ['job' => $job, 'exception' => $e]);
                                }
                            } catch (Throwable $e) {
                                $logger->alert("Job failed with errors, exiting.", [
                                    'name' => $job['name'],
                                    'id' => $job['jobID'],
                                    'extraParams' => $extraParams,
                                    'exception' => $e,
                                ]);
                                // Worker had a fatal error -- mark as failed.
                                try {
                                    $jobs->failJob($job['jobID']);
                                } catch (IllegalAction $e) {
                                    // IllegalAction is returned when we try to finish a job that's not RUNNING, this
                                    // can happen if we retried the command in a different server
                                    // after the first server actually ran the command (but we lost the response).
                                    $logger->info('Failed to FailJob we probably retried a repeat command so it is safe to ignore', ['job' => $job, 'exception' => $e]);
                                }
                            } finally {
                                if ($enableLoadHandler) {
                                    $localDB->open();
                                    $time = microtime(true);
                                    //$logger->info('[AIMD2] Setting finish time for job.', ['localJobID' => $localJobID, 'ended' => $time]);
                                    $stats->benchmark('bedrockWorkerManager.db.write.update', function () use ($localDB, $localJobID, $time) {
                                        $q = "UPDATE localJobs SET ended=".$time." WHERE localJobID=$localJobID;";
                                        //echo $q . "\n";
                                        $localDB->write($q);
                                    });
                                    $logger->info('Updating local db', ['total' => microtime(true) - $time]);
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
 * @return how many jobs it is safe to queue,.
 */
function getNumberOfJobsToQueue(): int
{
    global $enableLoadHandler, $logger, $maxJobsForSingleRun, $localDB, $minSafeJobs, $target, $ssthresh,
    $steadyStateDuration, $backoffThreshold, $ssthreshDefault;
    // Ok, here's the algorithm for this. We're always either ramping up the number of simultaneous jobs, or cutting it
    // back. There are two methods for ramping up:
    // 1) Slow start. This is poorly named, because it actually tries to start quickly, and ramps up exponentially.
    // 2) Congestion avoidance. This is a slow trickle up.
    // There's only one way to slow down. If we have a "congestion event", we cut the target number of jobs to half the
    // currently running jobs. Note that we don't cut this to half the *target* jobs. If we have a target of 100, but
    // only 10 jobs are running and we're slowing down, we want to run 5 jobs, not 50.
    // 
    // The hard part here is determining when things are "slowing down" this used to be determined by "batch size", but
    // that's variable, and there's no guarantee that any particular jobs are finished, and so it's sort of unclear
    // what each "batch" is actually measuring.
    // What we have, in simplified form, is a table that looks like:
    // JobID | runtime
    // ------+--------
    //     1 |      5s
    //     2 |      7s
    //     3 |    NULL
    //     4 |      6s
    //     5 |    NULL
    // etc.
    // There can be up to $target NULL lines in here at any given time (jobs in progress), which will be biased toward
    // the end of the list. We also have the actual start and end time of all these jobs, so we can do things like only
    // consider jobs that have run recently.
    //
    // How do we know if we're speeding up or slowing down, and how do we know when we're slowing down, if we're
    // slowing down by enough to cut our target? We'd like to react quickly, because if we are starting to overload
    // external systems, we'd like to react before that load starts causing other problems. It looks like most of our
    // jobs complete within hundreds of milliseconds, and so we probably have granular enough data with job timing to
    // get something useful out of that.
    // We don't want to keep *too* long of a job history, because we don't want to be hampered when switching job
    // types. I.e., we don't want to switch from processing mostly jobs that take 0.5s to mostly jobs that take 1s, and
    // be stuck forever thinking they're slow because we're comparing to hours of data on old jobs.
    //
    // I like the idea of using a timing based approach here - it scales automatically to any number of jobs we're
    // currently running, and we can guarantee a reaction time in a fixed time window, rather than across a variable
    // length number of jobs.
    // I'm proposing this as the fallback criteria:
    // On each iteration, we:
    // 1) average the duration of all jobs finished in the last 10 seconds.
    // 2) average the duration of all jobs finished during the previous 10 seconds.
    // 3) If either of the above numbers are 0, use the default safe number of jobs.
    // 4) If the average duration of jobs finished in the last 10 seconds is more than N * the duration of jobs
    //    finished in the previous 10 seconds, halve the job target. Otherwise, increase the job target.
    //    The hard part of this is choosing the value of N. In the previous implementation, this was 1.1, which seemed
    //    too low. I'm proposing 1.5, based on little more than it being bigger than 1.1. Some considerations here are
    //    that we don't want this to be too slow that every little jitter in job timing causes us to halve our
    //    throughput, but even more importantly, that we react quickly enough that we don't just allow jobs to
    //    continually slow by Nx on every iteration. What typically happens when things back up is that they fall off a
    //    cliff, so to speak, and the time required to complete tasks grow exponentially, so I think this might work.

    // Even better, let's determine some timing for jobs that we will call the "steady state average job duration". Any
    // time the most recent jobs finished *faster* than the previous jobs, we set the steady state duration to the
    // average of the jobs in both groups. The idea here is that jitter up and down in job completion time is normal,
    // and so as long as we see some jobs finishing faster than the previous ones, we assume we're on a fairly "level"
    // line as for job timing.
    // If we ever have a set of jobs that runs in more than N * the current steady state duration, we'll halve the
    // target. This keeps us from slowly but steadily increasing our timing by slightly less than N on each iteration,
    // though it's still vulnerable to edge cases (i.e., every even batch increases duration by 0.99*N, and every odd
    // batch decreases by 0.01N).
    // Even in this edge case, the safety valve kicks in when we've finished no jobs in 10 seconds and reset to our
    // base target.
    //
    // So, on each iteration:
    // Count the number of jobs that are currently running ($runningCount)
    // Count the jobs that finished in the last 10 seconds, and average their timing ($lastIntervalCount, $lastIntervalAverage)
    // Count the jobs that finished in the previous 10 seconds and average their timing ($previousIntervalCount, $previousIntervalAverage)
    // Delete any jobs that finished more than 20 seconds ago.
    // If either of these are 0, return the default value.
    // If $steadyStateDuration is 0, set it to $previousIntervalAverage (to initialize it on the first run). We set it
    // to the previous interval to avoid setting it to a value that might have only one or two data points, which could
    // cause an immediate backoff on the first interval.
    // If $lastIntervalAverage < $previousIntervalAverage update $steadyStateDuration.
    // If $lastIntervalAverage > $steadyStateDuration * N:
    //     $target = max($runningCount / 2, default min jobs)
    //     set $ssthresh to the new $target
    // Otherwise, increase $target by:
    //     if $target < $ssthresh, $target += min($lastIntervalCount, $target) // Don't increase by more jobs than
    //     finished, we shouldn't keep doubling the target if we're only actually running one job per interval.
    //     else target += 1.
    // return $target - $runningCount
    // Note: the whole point of $ssthresh is to speed up quickly at the beginning, it doesn't do anything after that.
    //
    // Properties of the backoff algorithm we want:
    // 1. It should be based on a relatively short history.
    //    We don't want to be backing off perpetually because we've switched to running jobs primarily of a slower
    //    type, that are constantly taking longer than a very long existing average.
    // 2. It should allow for a reasonable amount of jitter. Unlike TCP packet tramsmit times, we don't expect that
    //    subsequent jobs will take nearly exactly the same amount of time.
    // 3. It shouldn't allow for slow "Creep up". Allowing for each job or set of jobs to take slightly longer than the
    //    last one can result in run times slowing over time because none looks so much slower than the last the we
    //    reduce our target.
    // Figure out the times for the start and end of each interval.

    // Allow for disabling of the load handler.
    if (!$enableLoadHandler) {
        $logger->info('[AIMD2] Load handler not enabled, scheduling max jobs.', ['maxJobsForSingleRun' => $maxJobsForSingleRun]);

        return $maxJobsForSingleRun;
    }
    $now = microtime(true);
    $intervalDurationSeconds = 10;
    $oneIntervalAgo = $now - $intervalDurationSeconds;
    $twoIntervalsAgo = $oneIntervalAgo - $intervalDurationSeconds;

    // Look up how many jobs are currently in progress.
    $numActive = $localDB->read('SELECT COUNT(*) FROM localJobs WHERE ended IS NULL;')[0];

    // Look up how many jobs we've finished recently.
    $q0 = 'SELECT COUNT(*), AVG(ended - started) FROM localJobs WHERE ended > '.$oneIntervalAgo.';';
    $temp0 = $localDB->read($q0);
    $lastIntervalCount = $temp0[0];
    $lastIntervalAverage = floatval($temp0[1]);

    $q1 = 'SELECT COUNT(*), AVG(ended - started) FROM localJobs WHERE ended > '.$twoIntervalsAgo.' AND ended < '.$oneIntervalAgo.';';
    $temp1 = $localDB->read($q1);
    $previousIntervalCount = $temp1[0];
    $previousIntervalAverage = floatval($temp1[1]);

    //$logger->info('[AIMD2] '.$q0);
    //$logger->info('[AIMD2] '.$q1);
    /*
    $logger->info('[AIMD2] Calculating number of jobs to run.', ['numActive' => $numActive,
                                                         'lastIntervalCount' => $lastIntervalCount,
                                                         'lastIntervalAverage' => $lastIntervalAverage,
                                                         'previousIntervalCount' => $previousIntervalCount,
                                                         'previousIntervalAverage' => $previousIntervalAverage,
                                                         'oneIntervalAgo' => $oneIntervalAgo,
                                                         'twoIntervalsAgo' => $twoIntervalsAgo]);
    */

    // Delete old stuff.
    $localDB->write('DELETE FROM localJobs WHERE ended IS NOT NULL AND ended < '.$twoIntervalsAgo.';');

    if ($lastIntervalCount === 0) {
        $logger->info('[AIMD2] No jobs finished this interval, returning default value.', [ 'minSafeJobs' => $minSafeJobs, 'returnValue' => max($target - $numActive, 0)]);
        return max($target - $numActive, 0);
    } else if ($previousIntervalCount === 0) {
        $logger->info('[AIMD2] No jobs finished previous interval, returning default value.', ['minSafeJobs' => $minSafeJobs, 'returnValue' => max($target - $numActive, 0)]);
        return max($target - $numActive, 0);
    }

    // Update steadyStateDurationif required.
    if ($steadyStateDuration === 0 && $previousIntervalAverage > 0) {
        $steadyStateDuration = $lastIntervalAverage;
        $logger->info('[AIMD2] Initializing steadyStateDuration', ['steadyStateDuration' => $steadyStateDuration]);
    } else if ($lastIntervalAverage < $previousIntervalAverage) {
        $newSteadyStateDuration = (($lastIntervalAverage * $lastIntervalCount) + ($previousIntervalAverage * $previousIntervalCount)) / ($lastIntervalCount + $previousIntervalCount);
        if($newSteadyStateDuration !== $steadyStateDuration) {
            $steadyStateDuration = $newSteadyStateDuration;
            $logger->info('[AIMD2] updating steadyStateDuration', ['steadyStateDuration' => $steadyStateDuration]);
        }
    }

    // Update our target.
    if ($lastIntervalAverage > ($steadyStateDuration * $backoffThreshold)) {
        // Backoff
        $oldTarget = $target;
        $target = max(intval($target / 2), $minSafeJobs);
        // If $target has dropped all the way back to the minimum, then we'll reset $ssthresh to try and do a slow
        // start again, otherwise we'll set $ssthresh to the current target, effectively disabling it.
        $ssthresh = $target == $minSafeJobs ? $ssthreshDefault : $target;
        if ($target !== $oldTarget) {
            $logger->info('[AIMD2] Backing off jobs target.', ['oldTarget' => $oldTarget,
                                                       'target' => $target,
                                                       'lastIntervalAverage' => $lastIntervalAverage,
                                                       'steadyStateDuration' => $steadyStateDuration,
                                                       'backoffThreshold' => $backoffThreshold]);
       }
    } else if ($target < $ssthresh) {
        // Slow Start
        $oldTarget = $target;
        // Set the target to double the number of jobs we're currently running, unless that's lower than the existing
        // target.
        // TODO: The amonut we adjsut by should be relative to difference in the duration. What if we can ramp up by
        // the multiplier every interval?
        $target = intval(max($target, $numActive * 1.25)); // 1.25 derived by experimentation.
        $logger->info('[AIMD2] Slow start ramp up.', ['oldTarget' => $oldTarget,
                                              'target' => $target,
                                              'lastIntervalCount' => $lastIntervalCount,
                                              'ssthresh' => $ssthresh]);
    } else {
        // Congestion Avoidance.
        // Add one to the current target, but don't go past double the number of currently running jobs, and don't
        // lower the current target.
        $target = max($target, min($target + 1, $numActive * 2));
        $logger->info('[AIMD2] Congestion Avoidance, incrementing target', ['target' => $target]);
    }

    // Now we know how many jobs we want to be running, and how many are running, so we can return the difference.
    $numJobsToRun = max($target - $numActive, 0);
    $logger->info('[AIMD2] Found number of jobs to run.', ['numJobsToRun' => $numJobsToRun,
                                                   'target' => $target,
                                                   'numActive' => $numActive, 'lastIntervalCount' => $lastIntervalCount]);

    return $numJobsToRun;
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
