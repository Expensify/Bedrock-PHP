<?php

declare(strict_types=1);

namespace Expensify\Bedrock;

use Expensify\Bedrock\Exceptions\BedrockError;
use Expensify\Bedrock\Exceptions\Jobs\RetryableException;
use Expensify\Bedrock\Exceptions\Jobs\TimedOut;
use Expensify\Bedrock\Exceptions\Jobs\VersionFileChanged;
use Expensify\Bedrock\Jobs\WorkerInterface;
use Throwable;
use Exception;

class BedrockWorkerManager
{
    /**
     * @var string|null
     */
    private $versionWatchFile;
    /**
     * @var int
     */
    private $maxLoad;
    /**
     * @var int
     */
    private $maxLoopIteration;

    /**
     * @var bool|int
     */
    private $versionWatchFileTimestamp;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $logger;
    /**
     * @var Stats\StatsInterface
     */
    private $stats;

    /**
     * @var Client
     */
    private $bedrock;

    /**
     * @var Jobs
     */
    private $jobs;

    /**
     * BedrockWorkerManager constructor.
     * @param string|null $versionWatchFile
     * @param int $maxLoad
     * @param int $maxLoopIteration
     * @param array $bedrockConfig
     */
    public function __construct($versionWatchFile = null, int $maxLoad = 0, int $maxLoopIteration = 1000, array $bedrockConfig = [])
    {
        Client::configure($bedrockConfig);

        $this->versionWatchFile = $versionWatchFile;
        $this->maxLoad = $maxLoad;
        $this->maxLoopIteration = $maxLoopIteration;
        $this->logger = Client::getLogger();
        $this->stats = Client::getStats();
        $this->versionWatchFileTimestamp = $versionWatchFile && file_exists($versionWatchFile) ? filemtime($versionWatchFile) : false;
    }

    /**
     * Starts processing jobs.
     *
     * @param string $workerPath
     * @param string $jobName
     */
    public function start(string $workerPath, string $jobName)
    {
        $this->logger->info('Starting BedrockWorkerManager', ['maxLoopIteration' => $this->maxLoopIteration]);
        try {
            $this->bedrock = new Client();
            $this->jobs = new Jobs($this->bedrock);

            // Begin the infinite loop. We only exit it when there's an
            $loopIteration = 0;
            while (true) {
                if ($loopIteration === $this->maxLoopIteration) {
                    $this->logger->info("We did all our loops iteration, shutting down");
                    return;
                }

                $loopIteration++;
                $this->logger->info("Loop iteration", ['iteration' => $loopIteration]);

                $this->waitForAvailableLoad();

                try {
                    $response = $this->getJob($jobName);
                } catch (VersionFileChanged $e) {
                    // We break out of this loop and just wait for child processes to finish.
                    $this->logger->info('Version watch file changed, stop processing new jobs');
                    break;
                }

                $this->processJob($workerPath, $response['body']);
            }

            // We wait for all children to finish before dying.
            $status = null;
            pcntl_wait($status);
        } catch (Throwable $t) {
            $this->logger->alert('BedrockWorkerManager.php exited abnormally', ['exception' => $t]);
        }
    }

    /**
     * Waits till the system is not loaded.
     */
    private function waitForAvailableLoad()
    {
        while (true) {
            // Get the latest load
            $load = sys_getloadavg()[0];
            if ($load < $this->maxLoad) {
                $this->logger->info('Load is under max, checking for more work.', ['load' => $load, 'MAX_LOAD' => $this->maxLoad]);
                break;
            } else {
                $this->logger->info('Load is over max, waiting 1s and trying again.', ['load' => $load, 'MAX_LOAD' => $this->maxLoad]);
                sleep(1);
            }
        }
    }

    /**
     * Gets a job. It waits and keeps retrying until it finds one or throws if the version file changed.
     *
     * @param string $jobName
     * @return array
     */
    private function getJob(string $jobName): array
    {
        $response = null;
        while (!$response) {
            $this->checkVersionChange();
            try {
                // Attempt to get a job
                $response = $this->jobs->getJob($jobName, 60 * 1000); // Wait up to 60s
            } catch (TimedOut $e) {
                // There were no jobs to process, we'll try again
                $this->logger->info('No jobs found', ['message' => $e->getMessage()]);
            } catch (BedrockError $e) {
                // Try again in 60 seconds
                $this->logger->info('Problem getting job, retrying in 60s', ['message' => $e->getMessage()]);
                sleep(60);
            }
        }

        return $response;
    }

    /**
     * Checks if the version watch file changed.
     *
     * @throws VersionFileChanged
     */
    private function checkVersionChange()
    {
        if (!$this->versionWatchFile) {
            return;
        }

        // php's filemtime results are cached, so we need to clear that cache or we'll be getting a stale modified time.
        clearstatcache(true, $this->versionWatchFile);
        $newVersionWatchFileTimestamp = file_exists($this->versionWatchFile) ? filemtime($this->versionWatchFile) : false;
        if ($newVersionWatchFileTimestamp !== $this->versionWatchFileTimestamp) {
            throw new VersionFileChanged("Version file $this->versionWatchFile changed");
        }
    }

    /**
     * Forks a thread and processes one job in that thread.
     *  BWM jobs are '/' separated names, the last component of which indicates the name of the worker to
     * instantiate to execute this job: arbitrary/optional/path/to/workerName
     * We look for a file:
     *                    <workerPath>/<workerName>.php
     *                If it's there, we include it, and then create
     *                an object and run it like:
     *                    $worker = new $workerName( $job );
     *                    $worker->run( );
     * The optional path info allows for jobs to be scheduled selectively. I.e., you may have separate jobs
     * scheduled as production/jobName and staging/jobName, with a WorkerManager in each environment looking for
     * each path.
     *
     * @param string $workerPath
     * @param array $job
     * @throws Exception
     */
    private function processJob(string $workerPath, array $job)
    {
        $parts = explode('/', $job['name']);
        $jobParts = explode('?', $job['name']);
        $extraParams = count($jobParts) > 1 ? $jobParts[1] : [];
        $job['name'] = $jobParts[0];
        $workerName = $parts[count($parts) - 1];
        $workerName = explode('?', $workerName)[0];
        $workerFilename = $workerPath . "/$workerName.php";
        $this->logger->info("Looking for worker '$workerFilename'");
        if (!file_exists($workerFilename)) {
            // No worker for this job
            $this->logger->warning('No worker found, ignoring', ['jobName' => $job['name']]);
            $this->jobs->failJob($job['jobID']);
            return;
        }

        // The file exists -- fork it so we can run it.
        $this->logger->info("Forking and running a worker.", [
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
        }
        if ($pid == 0) {
            // We forked successfully
            $this->stats->counter('bedrockJob.create.' . $job['name']);
            $this->stats->benchmark('bedrockJob.finish.' . $job['name'], function () use ($workerName, $workerFilename, $job, $extraParams) {
                $this->runJob($workerName, $workerFilename, $job, $extraParams);
            });
            $this->stats->counter('bedrockJob.finish.' . $job['name']);

            // We are in the child process, so we can exit
            exit(1);
        }

        // Otherwise we are the parent thread -- continue execution
        $this->logger->info("Successfully started running job", [
            'name' => $job['name'],
            'id' => $job['jobID'],
            'pid' => $pid,
        ]);
    }

    /**
     * Runs a job and finishes/fails/retries the job depending on its result.
     *
     * @param string $workerName
     * @param string $workerFilename
     * @param array $job
     * @param array $extraParams
     */
    private function runJob(string $workerName, string $workerFilename, array $job, array $extraParams)
    {
        $this->logger->info("Running job", [
            'name' => $job['name'],
            'id' => $job['jobID'],
            'extraParams' => $extraParams,
        ]);
        $worker = $this->getWorkerInstance($workerName, $workerFilename, $job);
        try {
            // Run the worker.  If it completes successfully, finish the job.
            $worker->run();

            // Success
            $this->logger->info("Job completed successfully, exiting.", [
                'name' => $job['name'],
                'id' => $job['jobID'],
                'extraParams' => $extraParams,
            ]);
            $this->jobs->finishJob($job['jobID'], $worker->getData());
        } catch (RetryableException $e) {
            // Worker had a recoverable failure; retry again later.
            $this->logger->info("Job could not complete, retrying.", [
                'name' => $job['name'],
                'id' => $job['jobID'],
                'extraParams' => $extraParams,
            ]);
            $this->jobs->retryJob($job['jobID'], $e->getDelay(), $worker->getData());
        } catch (Throwable $e) {
            $this->logger->alert("Job failed with errors, exiting.", [
                'name' => $job['name'],
                'id' => $job['jobID'],
                'extraParams' => $extraParams,
                'exception' => $e,
            ]);
            // Worker had a fatal error -- mark as failed.
            $this->jobs->failJob($job['jobID']);
        }
    }

    /**
     * Instantiates a worker instance.
     *
     * @param string $workerName
     * @param string $workerFilename
     * @param array $job
     *
     * @return WorkerInterface
     */
    private function getWorkerInstance(string $workerName, string $workerFilename, array $job): WorkerInterface
    {
        include_once $workerFilename;
        return new $workerName($this->bedrock, $job);
    }
}
