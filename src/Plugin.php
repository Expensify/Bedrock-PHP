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
     * @var null|Client
     */
    protected $client = null;

    /**
     * Constructor.
     *
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }
}
