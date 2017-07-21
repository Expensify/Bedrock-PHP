<?php

declare(strict_types=1);

namespace Expensify\Bedrock;

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

    /**
     * Creates a localDB object and sets the file location.
     *
     * @param string $location
     */
    public function __construct(string $location)
    {
        $this->location = $location;
    }

    /**
     * Opens a DB connection.
     */
    public function open() {
        if (!isset($this->handle)) {
            $this->handle = new SQLite3($this->location);
            $this->handle->busyTimeout(15000);
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
     * @param string $query
     *
     * @return array|null
     */
    public function read(string $query)
    {
        $result = $this->handle->query($query);

        if ($result) {
            $returnValue = $result->fetchArray(SQLITE3_NUM);
        }

        return $returnValue ?? null;
    }

    /**
     * Runs a write query on a local database.
     *
     * @param string $query
     */
    public function write(string $query)
    {
        $this->handle->query($query);
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
