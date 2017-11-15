<?php

namespace Expensify\Bedrock\DB;

use Countable;
use JsonSerializable;

/**
 * Base class for Bedrock plugins.
 */
class Response implements JsonSerializable, Countable
{
    protected $container = null;

    public function __construct(array $leValue)
    {
        $this->container = $leValue;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return json_encode($this->toArray());
    }

    /**
     * @return array
     */
    public function toArray()
    {
        return $this->container;
    }

    /**
     * @return int
     */
    public function count()
    {
        return count($this->container);
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return $this->toArray();
    }

    /**
     * Shortcut method to access the value inside the container.
     *
     * @param string|array $index
     * @param mixed        $default
     *
     * @return mixed
     */
    protected function getFromContainer($index, $default = null)
    {
        if (is_array($index)) {
            $array = $this->container;
            foreach ($index as $step) {
                if (!isset($array[$step])) {
                    return $default;
                }
                $array = $array[$step];
            }

            return $array;
        }

        if (!is_string($index) && !is_int($index)) {
            return $default;
        }

        if (!isset($this->container[$index])) {
            return $default;
        }

        return $this->container[$index];
    }

    /**
     * Shortcut function to set data inside the container.
     *
     * @param string|int|array $index
     * @param mixed            $value
     *
     * @return mixed
     */
    protected function setContainerValue($index, $value)
    {
        return $this->container[$index] = $value;
    }

    /**
     * Shortcut to make sure the value of an index is strictly true.
     *
     * @param string |array $index
     *
     * @return bool
     */
    protected function isTrue($index)
    {
        return $this->getFromContainer($index) === true;
    }

    /**
     * Get the resposne code.
     *
     * @return int
     */
    public function getCode()
    {
        return (int) $this->getFromContainer('code');
    }

    /**
     * Get the headers (column names).
     *
     * @return string[]
     */
    public function getHeaders()
    {
        return $this->getFromContainer(['body', 'headers'], []);
    }

    /**
     * Get the rows returned.
     *
     * @param bool $assoc Do we want to return each row keyed by the column name? If not, they will be keyed by index.
     *
     * @return array[]
     */
    public function getRows(bool $assoc = false): array
    {
        $rows = $this->getFromContainer(['body', 'rows'], []);
        if (!$assoc) {
            return $rows;
        }
        $results = [];
        $headers = $this->getHeaders();
        foreach ($rows as $row) {
            $result = [];
            foreach ($headers as $index => $header) {
                $result[$header] = $row[$index];
            }
            $results[] = $result;
        }
        return $results;
    }

    /**
     * Get the error message.
     *
     * @return string
     */
    public function getError()
    {
        return $this->getFromContainer(['headers', 'error'], "");
    }

    /**
     * Returns true if no rows were returned.
     *
     * @return bool
     */
    public function isEmpty()
    {
        return count($this->getRows()) === 0;
    }

    /**
     * Returns the last insert row ID.
     *
     * @return int
     */
    public function getLastInsertRowID()
    {
        return $this->getFromContainer(['headers', 'lastInsertRowID']);
    }
}
