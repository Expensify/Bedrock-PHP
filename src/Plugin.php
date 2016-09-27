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
    protected $parent = null;

    /**
     * Constructor.
     *
     * @param Client $parent
     */
    public function __construct(Client $parent)
    {
        // Initialize
        $this->parent = $parent;
    }
}
