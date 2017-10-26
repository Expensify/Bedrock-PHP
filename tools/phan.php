#!/usr/bin/env php
<?php
declare(strict_types=1);
require realpath(dirname(__FILE__)).'/../_autoload.php';
use Expensify\Libs\PhanAnalyzer;
$analyzer = new PhanAnalyzer($_SERVER['TRAVIS_BRANCH']);
if ($analyzer->analyze()) {
    exit(0);
} else {
    exit(1);
}