#!/usr/bin/env php
<?php
require realpath(dirname(__FILE__)).'/../vendor/autoload.php';
use Expensify\Libs\PHPStyler;
$styler = new PHPStyler($_SERVER['TRAVIS_BRANCH'], $_SERVER['TRAVIS_COMMIT']);
$valid = $styler->check();
exit((int) !$valid);