#!/usr/bin/env php
<?php

declare(strict_types=1);
require realpath(dirname(__FILE__)).'/../vendor/autoload.php';
use Expensify\Bedrock\CI\PHPStyler;

// actions/checkout does not create the pull request ref locally, and the styler checks it out by name.
if (strpos($_SERVER['GITHUB_REF'], 'refs/pull/') === 0) {
    echo "Running git fetch origin {$_SERVER['GITHUB_REF']}:{$_SERVER['GITHUB_REF']}\n";
    shell_exec('git fetch --no-tags origin '.$_SERVER['GITHUB_REF'].':'.$_SERVER['GITHUB_REF']);
}

$styler = new PHPStyler($_SERVER['GITHUB_REF'], $_SERVER['GITHUB_SHA']);
$valid = $styler->check();
exit((int) !$valid);
