<?php

namespace Expensify\Bedrock;

use Exception;
use Expensify\Bedrock\Exceptions\Jobs\DoesNotExist;
use Expensify\Bedrock\Exceptions\Jobs\GenericError;
use Expensify\Bedrock\Exceptions\Jobs\IllegalAction;
use Expensify\Bedrock\Exceptions\Jobs\MalformedAttribute;
use Expensify\Bedrock\Exceptions\Jobs\SqlFailed;

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
     * State of a job that is currently queued.
     *
     * @var string
     */
    const STATE_QUEUED = "QUEUED";

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
        $this->client->getStats()->counter('bedrockJob.call.'.$method);
        $response = $this->client->getStats()->benchmark('bedrock.jobs.'.$method, function () use ($method, $headers, $body) {
            return $this->client->call($method, $headers, $body);
        });

        $job = isset($headers['name']) ? $headers['name'] : $headers['jobID'];
        $responseCode = isset($response['code']) ? $response['code'] : null;
        $codeLine = isset($response['codeLine']) ? $response['codeLine'] : null;

        $this->client->getStats()->counter('bedrockJob.call.response.'.$method.$responseCode);

        if ($responseCode!=200 && @$response['lastTryException']) {
            // We had a connection failure last time around, so let's ignore
            // any non-200 requests this time.  Either it worked and this error
            // is a false alarm, or it didn't work and the next command will
            // fail.  Either way, we can't know at this moment if this is a
            // real error so just log the problem and hope it gets sorted out.
            $lastError = $response['lastTryException']->getMessage();
            $this->client->getLogger()->warning("Retried a command and got a potentially benign error: '$lastError'");
            $responseCode = 200;
            $response['code'] = 200;
        }
            

        if ($responseCode === 402) {
            throw new MalformedAttribute("Malformed attribute. Job :$job, message: $codeLine");
        }

        if ($responseCode === 404) {
            throw new DoesNotExist("Job $job does not exist");
        }

        if ($responseCode === 405) {
            throw new IllegalAction("Cannot perform `$method` on job $job in a running state");
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
     * @param array|null  $data         (optional)
     * @param string|null $firstRun     (optional)
     * @param string|null $repeat       (optional) see https://github.com/Expensify/Bedrock/blob/master/plugins/Jobs.md#repeat-syntax
     * @param bool        $unique       Do we want only one job with this name to exist?
     * @param int         $priority     (optional) Specify a job priority. Jobs with higher priorities will be run first.
     * @param int|null    $parentJobID  (optional) Specify this job's parent job.
     * @param string      $connection   (optional) Specify 'Connection' header using constants defined in this class.
     * @param bool        $isIdempotent (optional) Whether the command can be retried if it fails.
     *
     * @return array Containing "jobID"
     */
    public function createJob($name, $data = null, $firstRun = null, $repeat = null, $unique = false, $priority = self::PRIORITY_MEDIUM, $parentJobID = null, $connection = self::CONNECTION_WAIT, $isIdempotent = false)
    {
        $this->client->getLogger()->info("Create job", ['name' => $name]);

        return $this->call(
            'CreateJob',
            [
                'name'        => $name,
                'data'        => $data,
                'firstRun'    => $firstRun,
                'repeat'      => $repeat,
                'unique'      => $unique,
                'priority'    => $priority,
                'parentJobID' => $parentJobID,
                'Connection'  => $connection,
                'idempotent'  => $isIdempotent
            ]
        );
    }

    /**
     * Waits for a match (if requested) and atomically dequeues exactly one job.
     *
     * @param string $name
     * @param int    $timeout (optional)
     *
     * @return array Containing all job details
     */
    public function getJob($name, $timeout = 0)
    {
        $headers = ["name" => $name];
        if ($timeout) {
            // Add the timeout
            $headers["Connection"] = "wait";
            $headers["timeout"]    = $timeout;
            $headers["idempotent"] = true;
        }

        return $this->call("GetJob", $headers);
    }

    /**
     * Updates the data associated with a job.
     *
     * @param int   $jobID
     * @param array $data
     * @param string $repeat      (optional) see https://github.com/Expensify/Bedrock/blob/master/plugins/Jobs.md#repeat-syntax
     *
     * @return array
     */
    public function updateJob($jobID, $data, $repeat = null)
    {
        return $this->call(
            "UpdateJob",
            [
                "jobID"      => $jobID,
                "data"       => $data,
                "repeat"     => $repeat,
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
                "jobID"      => $jobID,
                "data"       => $data,
                "idempotent" => true,
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
                "jobID"      => $jobID,
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
                "jobID"      => $jobID,
                "idempotent" => true,
            ]
        );
    }

    /**
     * Retry a job. Job must be in a RUNNING state to be able to be retried.
     *
     * @param int   $jobID
     * @param int   $delay
     * @param array $data
     *
     * @return array
     */
    public function retryJob($jobID, $delay = 0, $data = [])
    {
        return $this->call(
            "RetryJob",
            [
                "jobID"      => $jobID,
                "delay"      => $delay,
                "data"       => $data,
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
                "jobID"      => $jobID,
                "idempotent" => true,
            ]
        );

        return isset($bedrockResponse['body']) ? $bedrockResponse['body'] : null;
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
    public static function queueJob($name, $data = null, $firstRun = null, $repeat = null, $unique = false, $priority = self::PRIORITY_MEDIUM, $parentJobID = null, $connection = self::CONNECTION_WAIT)
    {
        try {
            $bedrock = new Client();
            $jobs = new self($bedrock);

            return $jobs->createJob($name, $data, $firstRun, $repeat, $unique, $priority, $parentJobID, $connection);
        } catch (Exception $e) {
            Client::getLogger()->alert('Could not create Bedrock job', ['exception' => $e]);

            return [];
        }
    }
}
