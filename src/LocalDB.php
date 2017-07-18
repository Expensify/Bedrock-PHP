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

    /**
     * Creates a LocalDB object and opens a database connection.
     *
     * @param string $location
     */
    public function __construct(string $location)
    {
        $this->handle = new SQLite3($location);
        $this->handle->busyTimeout(15000);
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
     * @return int
     */
    public function getLastInsertedRowID() : int
    {
        return $this->handle->lastInsertRowID();
    }
}
