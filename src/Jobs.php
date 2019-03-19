<?php

namespace Expensify\Bedrock;

use Exception;
use Expensify\Bedrock\Exceptions\Jobs\DoesNotExist;
use Expensify\Bedrock\Exceptions\Jobs\GenericError;
use Expensify\Bedrock\Exceptions\Jobs\IllegalAction;
use Expensify\Bedrock\Exceptions\Jobs\MalformedAttribute;
use Expensify\Bedrock\Exceptions\Jobs\SqlFailed;
use stdClass;

/**
 * Encapsulates the built-in Jobs plugin to Bedrock.
 *
 * @see https://github.com/Expensify/Bedrock/blob/master/plugins/Jobs.md
 */
class Jobs extends Plugin
{
    /**
     * Date format for $firstRun.
     *
     * @var string
     */
    const DATE_FORMAT = 'Y-m-d H:i:s';

    /**
     * State of a job that is currently running.
     *
     * @var string
     */
    const STATE_RUNNING = "RUNNING";

    /**
     * State of a job that is currently in the runqueued state.
     */
    const STATE_RUNQUEUED = 'RUNQUEUED';

    /**
     * State of a job that is currently queued.
     *
     * @var string
     */
    const STATE_QUEUED = "QUEUED";

    /**
     * State of a job that is currently cancelled.
     *
     * @var string
     */
    const STATE_CANCELLED = "CANCELLED";

    /**
     * State of a job that is currently failed.
     *
     * @var string
     */
    const STATE_FAILED = "FAILED";

    /**
     * "Connection" header option to wait for a response.
     *
     * @var string
     */
    const CONNECTION_WAIT = "wait";

    /**
     * "Connection" header option to forget and not wait for a response.
     *
     * @var string
     */
    const CONNECTION_FORGET = "forget";

    /**
     * Constant for the high priority.
     */
    const PRIORITY_HIGH = 1000;

    /**
     * Constant for the medium priority.
     */
    const PRIORITY_MEDIUM = 500;

    /**
     * Constant for the low priority.
     */
    const PRIORITY_LOW = 0;

    /**
     * Calls the Jobs plugin.
     *
     * @param string $method  Method to call
     * @param array  $headers Headers to send
     * @param string $body    Body of the request
     *
     * @throws DoesNotExist
     * @throws IllegalAction
     * @throws MalformedAttribute
     * @throws SqlFailed
     * @throws GenericError
     *
     * @return array
     */
    public function call($method, $headers = [], $body = '')
    {
        // If we ever pass an empty array as data, PHP will json encode it as an array, but data expects an object and
        // will generate a warning if it receives an array, so we pass an stdClass, which will get encoded as object
        if (isset($headers['data']) && is_array($headers['data']) && empty($headers['data'])) {
            $headers['data'] = new stdClass();
        }

        $this->client->getStats()->counter('bedrockJob.call.'.$method);
        $response = $this->client->getStats()->benchmark('bedrock.jobs.'.$method, function () use ($method, $headers, $body) {
            return $this->client->call($method, $headers, $body);
        });

        $job = $headers['name'] ?? $headers['jobID'] ?? null;
        $responseCode = $response['code'] ?? null;
        $codeLine = $response['codeLine'] ?? null;

        $this->client->getStats()->counter('bedrockJob.call.response.'.$method.$responseCode);

        if ($responseCode === 402) {
            throw new MalformedAttribute("Malformed attribute. Job :$job, message: $codeLine");
        }

        if ($responseCode === 404) {
            throw new DoesNotExist("Job $job does not exist");
        }

        if ($responseCode === 405) {
            throw new IllegalAction("Cannot perform `$method` on job $job");
        }

        if ($responseCode === 502) {
            throw new SqlFailed("SQL failed for job $job: {$codeLine}");
        }

        // 202 code is a successful job creation using the "Connection: forget" header
        if (!in_array($responseCode, [200, 202])) {
            throw new GenericError("Generic error for job $job");
        }

        return $response;
    }

    /**
     * Schedules a new job, optionally in the future, optionally to repeat.
     *
     * @param string      $name
     * @param array|null  $data        (optional)
     * @param string|null $firstRun    (optional)
     * @param string|null $repeat      (optional) see https://github.com/Expensify/Bedrock/blob/master/plugins/Jobs.md#repeat-syntax
     * @param bool|null   $unique      (optional) Do we want only one job with this name to exist?
     * @param int|null    $priority    (optional) Specify a job priority. Jobs with higher priorities will be run first.
     * @param int|null    $parentJobID (optional) Specify this job's parent job.
     * @param string|null $connection  (optional) Specify 'Connection' header using constants defined in this class.
     * @param string|null $retryAfter  (optional) Specify after what time in RUNNING this job should be retried (same syntax as repeat)
     *
     * @return array Containing "jobID"
     */
    public function createJob($name, $data = null, $firstRun = null, $repeat = null, $unique = false, $priority = self::PRIORITY_MEDIUM, $parentJobID = null, $connection = self::CONNECTION_WAIT, $retryAfter = null)
    {
        $this->client->getLogger()->info("Create job", ['name' => $name]);
        $commitCounts = Client::getCommitCounts();

        $response = $this->call(
            'CreateJob',
            [
                'name' => $name,
                'data' => array_merge($data ?? [], count($commitCounts) ? ['_commitCounts' => $commitCounts] : []),
                'firstRun' => $firstRun,
                'repeat' => $repeat,
                'unique' => $unique,
                'jobPriority' => $priority,
                'parentJobID' => $parentJobID,
                'Connection' => $connection,
                // If the name of the job has to be unique, Bedrock will return any existing job that exists with the
                // given name instead of making a new one, which essentially makes the command idempotent.
                'idempotent' => $unique,
                'retryAfter' => $retryAfter,
            ]
        );

        $this->client->getLogger()->info('Job created', ['name' => $name, 'id' => $response['body']['jobID'] ?? null]);

        return $response;
    }

    /**
     * Schedules a list of jobs.
     *
     * @param array $jobs JSON array containing each job. Each job should include the same parameters as jobs define in CreateJob
     *
     * @return array - contain the jobIDs with the unique identifier of the created jobs
     */
    public function createJobs(array $jobs): array
    {
        $this->client->getLogger()->info("Create jobs", ['jobs' => $jobs]);

        // We renamed the `priority` param to `jobPriority` because `priority` is a generic param of any bedrock command.
        foreach ($jobs as $i => $job) {
            if (isset($jobs[$i]['priority'])) {
                $jobs[$i]['jobPriority'] = $jobs[$i]['priority'];
                unset($jobs[$i]['priority']);
            }
        }

        // If the name of the job has to be unique, Bedrock will return any existing job that exists with the
        // given name instead of making a new one, which essentially makes the command idempotent.
        $areAllJobsUnique = true;
        $commitCounts = Client::getCommitCounts();
        foreach ($jobs as $i => $job) {
            $jobs[$i]['data'] = array_merge($jobs[$i]['data'] ?? [], count($commitCounts) ? ['_commitCounts' => $commitCounts] : []);
            $areAllJobsUnique = ($jobs[$i]['unique'] ?? false) && $areAllJobsUnique;
        }

        $response = $this->call(
            'CreateJobs',
            [
                'jobs' => $jobs,
                'idempotent' => $areAllJobsUnique,
            ]
        );

        $this->client->getLogger()->info('Jobs created', ['jobIDs' => $response['body']['jobIDs'] ?? null]);

        return $response;
    }

    /**
     * Waits for a match (if requested) and atomically dequeues exactly one job.
     *
     * @param string $name
     *
     * @return array Containing all job details
     */
    public function getJob($name)
    {
        $headers = ["name" => $name];

        return $this->call("GetJob", $headers);
    }

    /**
     * Waits for a match (if requested) and atomically dequeues $numResults jobs.
     *
     * @param string $name
     * @param int    $numResults
     *
     * @return array Containing all job details
     */
    public function getJobs(string $name, int $numResults, array $params = []): array
    {
        $headers = [
            "name" => $name,
            "numResults" => $numResults,
        ];

        $headers = array_merge($headers, $params);

        return $this->call("GetJobs", $headers);
    }

    /**
     * Updates the data associated with a job.
     *
     * @param int    $jobID
     * @param array  $data
     * @param string $repeat (optional) see https://github.com/Expensify/Bedrock/blob/master/plugins/Jobs.md#repeat-syntax
     *
     * @return array
     */
    public function updateJob($jobID, $data, $repeat = null)
    {
        $commitCounts = Client::getCommitCounts();
        return $this->call(
            "UpdateJob",
            [
                "jobID" => $jobID,
                "data" => array_merge($data ?? [], count($commitCounts) ? ['_commitCounts' => $commitCounts] : []),
                "repeat" => $repeat,
                "idempotent" => true,
            ]
        );
    }

    /**
     * Marks a job as finished, which causes it to repeat if requested.
     *
     * @param int   $jobID
     * @param array $data  (optional)
     *
     * @return array
     */
    public function finishJob($jobID, $data = null)
    {
        return $this->call(
            "FinishJob",
            [
                "jobID" => $jobID,
                "data" => $data,
                "idempotent" => true,
            ]
        );
    }

    /**
     * Cancel a QUEUED, RUNQUEUED, FAILED job from a sibling.
     *
     * @param int $jobID
     *
     * @return array
     */
    public function cancelJob(int $jobID): array
    {
        return $this->call(
            "CancelJob",
            [
                "jobID" => $jobID,
            ]
        );
    }

    /**
     * Removes all trace of a job.
     *
     * @param int $jobID
     *
     * @return array
     */
    public function deleteJob($jobID)
    {
        return $this->call(
            "DeleteJob",
            [
                "jobID" => $jobID,
                "idempotent" => true,
            ]
        );
    }

    /**
     * Mark a job as failed.
     *
     * @param int $jobID
     *
     * @return array
     */
    public function failJob($jobID)
    {
        return $this->call(
            "FailJob",
            [
                "jobID" => $jobID,
                "idempotent" => true,
            ]
        );
    }

    /**
     * Retry a job. Job must be in a RUNNING state to be able to be retried.
     */
    public function retryJob(int $jobID, int $delay = 0, array $data = null, string $name = '', string $nextRun = ''): array
    {
        return $this->call(
            "RetryJob",
            [
                "jobID" => $jobID,
                "delay" => $delay,
                "data" => $data,
                "name" => $name,
                "nextRun" => $nextRun,
                "idempotent" => true,
            ]
        );
    }

    /**
     * Query a job's info.
     * Bedrock will return:
     *     - 200 - OK
     *         . created - creation time of this job
     *         . jobID - unique ID of the job
     *         . state - One of QUEUED, RUNNING, FINISHED
     *         . name  - name of the actual job matched
     *         . nextRun - timestamp of next scheduled run
     *         . lastRun - timestamp it was last run
     *         . repeat - recurring description
     *         . data - JSON data associated with this job.
     *
     * @param int $jobID
     *
     * @return array|null
     */
    public function queryJob($jobID)
    {
        $bedrockResponse = $this->call(
            "QueryJob",
            [
                "jobID" => $jobID,
                "idempotent" => true,
            ]
        );

        return $bedrockResponse['body'] ?? null;
    }

    /**
     * Queries several job's info.
     * Bedrock will return a jobs list with:
     *     - 200 - OK
     *         . created - creation time of this job
     *         . jobID - unique ID of the job
     *         . state - One of QUEUED, RUNNING, FINISHED
     *         . name  - name of the actual job matched
     *         . nextRun - timestamp of next scheduled run
     *         . lastRun - timestamp it was last run
     *         . repeat - recurring description
     *         . data - JSON data associated with this job.
     *
     * @param int[] $jobIDs
     *
     * @return array[]
     */
    public function queryJobs(array $jobIDs)
    {
        $bedrockResponse = $this->call(
            "QueryJobs",
            [
                "jobIDList" => implode(',', $jobIDs),
                "idempotent" => true,
            ]
        );

        return $bedrockResponse['body'] ?? [];
    }


    /**
     * Schedules a new job, optionally in the future, optionally to repeat.
     * Silently fails in case of an exception and logs the error.
     *
     * @param string      $name
     * @param array|null  $data        (optional)
     * @param string|null $firstRun    (optional)
     * @param string|null $repeat      (optional) see https://github.com/Expensify/Bedrock/blob/master/plugins/Jobs.md#repeat-syntax
     * @param bool        $unique      Do we want only one job with this name to exist?
     * @param int         $priority    (optional) Specify a job priority. Jobs with higher priorities will be run first.
     * @param int|null    $parentJobID (optional) Specify this job's parent job.
     * @param string      $connection  (optional) Specify 'Connection' header using constants defined in this class.
     *
     * @return array Containing "jobID"
     */
    public static function queueJob($name, $data = null, $firstRun = null, $repeat = null, $unique = false, $priority = self::PRIORITY_MEDIUM, $parentJobID = null, $connection = self::CONNECTION_WAIT, string $retryAfter = "")
    {
        $bedrock = Client::getInstance();
        try {
            $jobs = new self($bedrock);

            return $jobs->createJob($name, $data, $firstRun, $repeat, $unique, $priority, $parentJobID, $connection, $retryAfter);
        } catch (Exception $e) {
            $bedrock->getLogger()->alert('Could not create Bedrock job', ['exception' => $e]);

            return [];
        }
    }
}
