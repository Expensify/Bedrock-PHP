<?php

namespace Expensify\Bedrock\Stats;

interface StatsInterface
{
    public function counter($name, $value = 1);

    public function timer($name, $value);

    public function benchmark($name, callable $function);
}
