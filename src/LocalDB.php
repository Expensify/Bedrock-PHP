<?php

namespace Expensify\Bedrock;

use SQLite3;

class LocalDB
{
    private $location;

    /**
     * Creates a LocalDB object and defines the file location.
     */
    public function __construct($location)
    {
        $this->location = $location;
    }

    /**
     * Runs a read query on a local database.
     *
     * @param string $query
     *
     * @return array
     */
    public function read($query)
    {
        $handle = new SQLite3($this->location);
        $handle->busyTimeout(15000);
        $result = $handle->query($query);

        if ($result) {
            $returnValue = $result->fetchArray(SQLITE3_NUM);
        }

        $handle->close();
        unset($handle);
        return $returnValue ?? null;
    }

    /**
     * Runs a write query on a local database.
     *
     * @param string $query
     */
    public function write($query)
    {
        $handle = new SQLite3($this->location);
        $handle->busyTimeout(15000);
        $result = $handle->query($query);
        $handle->close();
        unset($handle);
    }
}
