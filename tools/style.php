#!/usr/bin/env php
<?php
declare(strict_types=1);
require realpath(dirname(__FILE__)).'/../_autoload.php';
use Expensify\Libs\PHPStyler;

$styler = new PHPStyler($_SERVER['TRAVIS_BRANCH'], $_SERVER['TRAVIS_COMMIT']);
$valid = $styler->check();
exit((int) !$valid);
