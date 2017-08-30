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
    /** @var SQLite3 $handle */
    private $handle;

    /** @var string $location */
    private $location;

    /** @var LoggerInterface $logger */
    private $logger;

    /**
     * Creates a localDB object and sets the file location.
     */
    public function __construct(string $location, LoggerInterface $logger)
    {
        $this->location = $location;
        $this->logger = $logger;
    }

    /**
     * Opens a DB connection.
     */
    public function open() {
        if (!isset($this->handle)) {
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
        if (isset($this->handle)) {
            $this->handle->close();
            unset($this->handle);
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
                $this->logger->info("Query failed, retrying", ['query' => $query, 'error' => json_encode($e)]);
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
                $this->logger->info("Query failed, retrying", ['query' => $query, 'error' => json_encode($e)]);
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
