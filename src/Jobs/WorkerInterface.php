<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Jobs;

use Expensify\Bedrock\Client;

/**
 * Interface that a worker needs to implement.
 */
interface WorkerInterface
{
    public function __construct(Client $client, array $job);

    /**
     * Runs the job.
     * If it throws a RetryableError then the job will be retried by BedrockWorkerManager.
     */
    public function run();

    /**
     * Returns the data of the job.
     * @return array
     */
    public function getData(): array;
}
