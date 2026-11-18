#!/usr/bin/env php
<?php

declare(strict_types=1);
require realpath(dirname(__FILE__)).'/../vendor/autoload.php';
use Expensify\Bedrock\CI\PhanAnalyzer;

// actions/checkout does not create the pull request ref locally, and the analyzer checks it out by name.
if (strpos($_SERVER['GITHUB_REF'], 'refs/pull/') === 0) {
    echo "Running git fetch origin {$_SERVER['GITHUB_REF']}:{$_SERVER['GITHUB_REF']}\n";
    shell_exec('git fetch --no-tags origin '.$_SERVER['GITHUB_REF'].':'.$_SERVER['GITHUB_REF']);
}

$analyzer = new PhanAnalyzer($_SERVER['GITHUB_REF']);
if ($analyzer->analyze()) {
    exit(0);
}
exit(1);
