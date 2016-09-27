<?php

namespace Expensify\Bedrock;

use Expensify\Bedrock\Exceptions\DB\FailedQuery;
use Expensify\Bedrock\Exceptions\DB\UnknownError;
use Expensify\Bedrock\DB\Response;

/**
 * Encapsulates the built-in DB plugin for Bedrock.
 *
 * @see https://github.com/Expensify/Server-Expensify/blob/master/bedrock/README.md
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
     *
     * @throws FailedQuery
     * @throws UnknownError
     */
    public function query($sql)
    {
        $response = new Response($this->parent->call(
            "Query",
            [
                "query"  => $sql,
                "format" => "json",
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
