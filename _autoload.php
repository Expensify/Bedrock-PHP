<?php

require realpath(dirname(__FILE__)).'/vendor/autoload.php';

spl_autoload_register('_expensify_bedrockphp_autoload');

/**
 * Custom class autloader. PHP5 is in da house.
 *
 * @param String $className
 */
function _expensify_bedrockphp_autoload($className)
{
    // Autoload file in the vendor PHP-Libs directory
    $classPathArray = explode('\\', $className);
    $filename = realpath(dirname(__FILE__)).'/vendor/expensify/PHP-Libs/src/'.str_replace("_", DIRECTORY_SEPARATOR, $classPathArray[count($classPathArray) - 1]).'.php';
    echo "\n\nTrying to autoload file: $filename\n\n";
    if (is_readable($filename)) {
        require $filename;

        return;
    }
}
