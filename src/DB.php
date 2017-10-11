<?php

namespace Expensify\Bedrock;

use Expensify\Bedrock\Exceptions\DB\FailedQuery;
use Expensify\Bedrock\Exceptions\DB\UnknownError;
use Expensify\Bedrock\DB\Response;

/**
 * Encapsulates the built-in DB plugin for Bedrock.
 *
 * @see https://github.com/Expensify/Bedrock/blob/master/README.md
 */
class DB extends Plugin
{
    /**
     * Ok response code.
     *
     * @var int
     */
    const CODE_OK = 200;

    /**
     * Failed query response code.
     *
     * @var int
     */
    const CODE_QUERY_FAILED = 502;

    /**
     * Executes a single SQL query.
     *
     * @param string $sql The query to run
     *
     * @return Response
     * @deprecated Use read or write methods instead.
     */
    public function query($sql)
    {
        if (preg_match('/^\s*SELECT.*/i', $sql)) {
            return $this->read($sql);
        }
        return $this->write($sql);
    }

    /**
     * Executes a single read SQL query.
     *
     * @param string $sql The query to run
     *
     * @return Response
     */
    public function read($sql)
    {
        return $this->_runQuery($sql, true);
    }

    /**
     * Executes a write read SQL query.
     *
     * @param string $sql The query to run
     *
     * @return Response
     */
    public function write($sql)
    {
        return $this->_runQuery($sql, false);
    }

    /**
     * Executes an SQL query.
     *
     * @param string $sql The query to run
     * @param bool $idempotent Is this command idempotent? Writes usually aren't.
     *
     * @return Response
     *
     * @throws FailedQuery
     * @throws UnknownError
     */
    private function _runQuery(string $sql, bool $idempotent)
    {
        $sql = substr($sql, -1) === ";" ? $sql : $sql.";";
        $response = new Response($this->client->call(
            'Query',
            [
                'query' => $sql,
                'format' => "json",
                'idempotent' => $idempotent,
            ]
        ));

        if ($response->getCode() === self::CODE_QUERY_FAILED) {
            throw new FailedQuery("Query failed: {$response->getError()}");
        }

        if ($response->getCode() !== self::CODE_OK) {
            throw new UnknownError("Unknown error: {$response->getError()}");
        }

        return $response;
    }
}
