<?php

declare(strict_types=1);

require realpath(dirname(__FILE__)).'/vendor/autoload.php';

spl_autoload_register('_expensify_bedrockphp_autoload');

function _expensify_bedrockphp_autoload(string $className)
{
    // Autoload file in the vendor PHP-Libs directory
    $classPathArray = explode('\\', $className);
    $filename = realpath(dirname(__FILE__)).'/lib/src/'.str_replace("_", DIRECTORY_SEPARATOR, $classPathArray[count($classPathArray) - 1]).'.php';
    if (is_readable($filename)) {
        require $filename;

        return;
    }
}
