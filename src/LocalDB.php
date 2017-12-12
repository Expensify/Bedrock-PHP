<?php

declare(strict_types=1);

namespace Expensify\Bedrock;

use Exception;
use Expensify\Bedrock\Stats\StatsInterface;
use Psr\Log\LoggerInterface;
use SQLite3;

/**
 * Class the represents a database on the local server.
 */
class LocalDB
{
    /** @var SQLite3 $handle */
    private $handle;

    /** @var string $location */
    private $location;

    /** @var LoggerInterface $logger */
    private $logger;

    /**
     * Creates a localDB object and sets the file location.
     */
    public function __construct(string $location, LoggerInterface $logger, StatsInterface $stats)
    {
        $this->location = $location;
        $this->logger = $logger;
        $this->stats = $stats;
    }

    /**
     * Opens a DB connection.
     */
    public function open() {
        if (!isset($this->handle)) {
            $startTime = microtime(true);
            $this->handle = new SQLite3($this->location);
            $this->handle->busyTimeout(15000);
            $this->handle->enableExceptions(true);
            $this->stats->timer('bedrockWorkerManager.db.open', microtime(true) - $startTime);
        }
    }

    /**
     * Close the DB connection and unset the object.
     */
    public function close()
    {
        if (isset($this->handle)) {
            $startTime = microtime(true);
            $this->handle->close();
            unset($this->handle);
            $this->stats->timer('bedrockWorkerManager.db.close', microtime(true) - $startTime);
        }
    }

    /**
     * Runs a read query on a local database.
     *
     * @return array|null
     */
    public function read(string $query)
    {
        $result = null;
        while(true) {
            try {
                $result = $this->handle->query($query);
                break;
            } catch (Exception $e) {
                if ($e->getMessage() === 'database is locked') {
                    $this->logger->info("Query failed, retrying", ['query' => $query, 'error' => $e->getMessage()]);
                } else {
                    $this->logger->info("Query failed, not retrying", ['query' => $query, 'error' => $e->getMessage()]);
                    throw $e;
                }
            }
        }

        if ($result) {
            $returnValue = $result->fetchArray(SQLITE3_NUM);
        }

        return $returnValue ?? null;
    }

    /**
     * Runs a write query on a local database.
     */
    public function write(string $query)
    {
        while(true) {
            try {
                $this->handle->query($query);
                break;
            } catch (Exception $e) {
                if ($e->getMessage() === 'database is locked') {
                    $this->logger->info("Query failed, retrying", ['query' => $query, 'error' => $e->getMessage()]);
                } else {
                    $this->logger->info("Query failed, not retrying", ['query' => $query, 'error' => $e->getMessage()]);
                    throw $e;
                }
            }
        }
    }

    /**
     * Gets last inserted row.
     *
     * @return int|null
     */
    public function getLastInsertedRowID()
    {
        if (!isset($this->handle)) {
            return null;
        }

        return $this->handle->lastInsertRowID();
    }
}
