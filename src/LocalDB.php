<?php

namespace Expensify\Bedrock;

use SQLite3;

class LocalDB
{
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
     * @return array
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
    public function getLastInsertedRowID()
    {
        return $this->handle->lastInsertRowID();
    }
}
