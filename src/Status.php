<?php

namespace Expensify\Bedrock;

/**
 * Encapsulates the built-in Status plugin for Bedrock.
 */
class Status extends Plugin
{
    /**
     * Generates a trivial response.
     *
     * @return array
     */
    public function ping()
    {
        return $this->client->call("Ping");
    }
}
