#!/usr/bin/env php
<?php

declare(strict_types=1);
require realpath(dirname(__FILE__)).'/../vendor/autoload.php';
use Expensify\Bedrock\CI\PhanAnalyzer;

$analyzer = new PhanAnalyzer($_SERVER['GITHUB_REF']);
if ($analyzer->analyze()) {
    exit(0);
}
exit(1);
