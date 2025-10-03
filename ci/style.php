#!/usr/bin/env php
<?php

declare(strict_types=1);
require realpath(dirname(__FILE__)).'/../vendor/autoload.php';
use Expensify\Bedrock\CI\PHPStyler;

$styler = new PHPStyler($_SERVER['GITHUB_REF'], $_SERVER['GITHUB_SHA']);
$valid = $styler->check();
exit((int) !$valid);
