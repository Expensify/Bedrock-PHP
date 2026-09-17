<?php

declare(strict_types=1);

use Expensify\Bedrock\Client;
use Expensify\Bedrock\Exceptions\Jobs\RetryableException;
use Expensify\Bedrock\Jobs;

final class SnapshotWorker
{
    public function __construct(private Client $client, private array $job)
    {
    }

    public function getParam(string $name)
    {
        return $this->job['data'][$name] ?? null;
    }

    public function getData(): array
    {
        return $this->job['data'];
    }

    public function run(): void
    {
        if (($this->job['data']['activity'] ?? 0) !== 1) {
            return;
        }

        // Enqueue new work after GetJobs and before BWM sends the worker's terminal request.
        $jobs = new Jobs($this->client);
        $jobs->createJob($this->job['name'], ['activity' => 2], unique: true, rerunIfDataChanged: true);
        $this->job['data']['workerProgress'] = 1;
        if ($this->job['data']['outcome'] === 'retry') {
            throw new RetryableException('Retry with progress');
        }
        if ($this->job['data']['outcome'] === 'fail') {
            throw new RuntimeException('Worker failed after a newer enqueue');
        }
    }
}
