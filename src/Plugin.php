<?php

namespace Expensify\Bedrock;

/**
 * Base class for Bedrock plugins.
 */
class Plugin
{
    /**
     * Pointer to parent Bedrock object.
     *
     * @var Client
     */
    protected $client;

    /**
     * Constructor.
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }
}
