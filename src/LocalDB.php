<?php

declare(strict_types=1);

namespace Expensify\Bedrock;

use Exception;
use Psr\Log\LoggerInterface;
use SQLite3;

/**
 * Class the represents a database on the local server.
 */
class LocalDB
{
    /** @var SQLite3|null */
    private $handle;

    /** @var string */
    private $location;

    /** @var LoggerInterface */
    private $logger;

    /** @var Stats\StatsInterface */
    private $stats;

    /**
     * Creates a localDB object and sets the file location.
     *
     * @param Stats\StatsInterface $stats
     */
    public function __construct(string $location, LoggerInterface $logger, $stats)
    {
        $this->location = $location;
        $this->logger = $logger;
        $this->stats = $stats;
    }

    /**
     * Opens a DB connection.
     */
    public function open()
    {
        if ($this->handle === null) {
            $this->handle = new SQLite3($this->location);
            $this->handle->busyTimeout(15000);
            $this->handle->enableExceptions(true);
        }
    }

    /**
     * Close the DB connection and unset the object.
     */
    public function close()
    {
        if ($this->handle !== null) {
            $this->handle->close();
            $this->handle = null;
        }
    }

    /**
     * Runs a read query on a local database.
     *
     * @return array
     */
    public function read(string $query)
    {
        $result = null;
        $returnValue = [];
        while (true) {
            try {
                $result = $this->handle->query($query);
                break;
            } catch (Exception $e) {
                if ($e->getMessage() === 'database is locked') {
                    $this->logger->info('Query failed, retrying', ['query' => $query, 'error' => $e->getMessage()]);
                } else {
                    $this->logger->info('Query failed, not retrying', ['query' => $query, 'error' => $e->getMessage()]);
                    throw $e;
                }
            }
        }

        if ($result) {
            $returnValue = $result->fetchArray(SQLITE3_NUM);
        }

        return $returnValue ?? [];
    }

    /**
     * Runs a read query and returns every matching row as a numeric array, unlike read() which
     * returns only the first row. Used for GROUP BY queries (e.g. per-type job stats).
     *
     * @return array<int, array<int, mixed>>
     */
    public function readAll(string $query): array
    {
        $result = null;
        while (true) {
            try {
                $result = $this->handle->query($query);
                break;
            } catch (Exception $e) {
                if ($e->getMessage() === 'database is locked') {
                    $this->logger->info('Query failed, retrying', ['query' => $query, 'error' => $e->getMessage()]);
                } else {
                    $this->logger->info('Query failed, not retrying', ['query' => $query, 'error' => $e->getMessage()]);
                    throw $e;
                }
            }
        }

        $rows = [];
        if ($result) {
            while (($row = $result->fetchArray(SQLITE3_NUM)) !== false) {
                $rows[] = $row;
            }
        }

        return $rows;
    }

    /**
     * Runs a write query on a local database.
     */
    public function write(string $query)
    {
        while (true) {
            try {
                $this->handle->query($query);
                break;
            } catch (Exception $e) {
                if ($e->getMessage() === 'database is locked') {
                    $this->logger->info('Query failed, retrying', ['query' => $query, 'error' => $e->getMessage()]);
                } else {
                    $this->logger->info('Query failed, not retrying', ['query' => $query, 'error' => $e->getMessage()]);
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
        if ($this->handle === null) {
            return null;
        }

        return $this->handle->lastInsertRowID();
    }
}
