#!/usr/bin/env php
<?php

declare(strict_types=1);
echo '1';
require realpath(dirname(__FILE__)).'/../vendor/autoload.php';
echo '2';
use Expensify\Bedrock\CI\PHPStyler;

echo '3';

$styler = new PHPStyler($_SERVER['GITHUB_REF'], $_SERVER['GITHUB_SHA']);
echo '4';
$valid = $styler->check();
echo '5';
echo $valid;
exit((int) !$valid);
