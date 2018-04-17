<?php

namespace Expensify\Bedrock;

use Expensify\Bedrock\DB\Response;
use Expensify\Bedrock\Exceptions\BedrockError;

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
     * @deprecated Use run method instead.
     */
    public function query($sql): Response
    {
        if (preg_match('/^\s*SELECT.*/i', $sql)) {
            return $this->run($sql, true);
        }
        return $this->run($sql, false);
    }

    /**
     * Executes an SQL query.
     *
     * @param string $sql        The query to run
     * @param bool   $idempotent Is this command idempotent? If the command is run twice is the final result the same?
     * @param int    $timeout    Time in microseconds, defaults to 60 seconds
     *
     * @throws BedrockError
     */
    public function run(string $sql, bool $idempotent, int $timeout = 60000000): Response
    {
        $sql = substr($sql, -1) === ";" ? $sql : $sql.";";
        $matches = [];
        preg_match('/\s*(select|insert|delete|update).*/i', $sql, $matches);
        $operation = isset($matches[1]) && in_array(strtolower($matches[1]), ['insert', 'update', 'delete', 'select']) ? strtolower($matches[1]) : 'unknown';
        $response = $this->client->getStats()->benchmark("bedrock.db.query.$operation", function () use ($sql, $idempotent, $timeout) {
            return new Response($this->client->call(
                'Query',
                [
                    'query' => $sql,
                    'format' => "json",
                    'idempotent' => $idempotent,
                    'timeout' => $timeout,
                ]
            ));
        });

        if ($response->getCode() === self::CODE_QUERY_FAILED) {
            throw new BedrockError($response->getCodeLine()." - ".$response->getError(), $response->getCode());
        }

        if ($response->getCode() !== self::CODE_OK) {
            throw new BedrockError($response->getCodeLine(), $response->getCode());
        }

        return $response;
    }
}
