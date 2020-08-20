<?php

namespace Expensify\Bedrock\Stats;

/**
 * Interface for sending stats about bedrock operations.
 */
interface StatsInterface
{
    /**
     * Count the times an event happened.
     *
     * @param string $name  Name of the event
     * @param int    $value Value of the counter
     */
    public function counter($name, $value = 1);

    /**
     * Track how long an event took.
     *
     * @param string $name  Name of the event
     * @param int    $value Duration in ms
     */
    public function timer($name, $value);

    /**
     * Benchmarks the execution of the passed function and tracks it as a `timer`.
     *
     * @param string   $name     Name of the event
     * @param callable $function Function to run
     *
     * @return mixed The return value of the passed function
     */
    public function benchmark($name, callable $function);
}
