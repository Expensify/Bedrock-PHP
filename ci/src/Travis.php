<?php

namespace Expensify\Bedrock\CI;

/**
 * Class Travis. Utility functions to format output for travis.
 */
class Travis
{
    const ANSI_CLEAR = "\033[0K";

    protected static $timerID;
    protected static $timerStart;

    public static function fold($action, $foldName)
    {
        echo "travis_fold:$action:$foldName\r".self::ANSI_CLEAR;
    }

    public static function foldCall($foldName, $cmd, $includeTimer = true)
    {
        self::fold("start", $foldName);

        if ($includeTimer) {
            self::timeStart();
        }

        system($cmd, $retVal);

        if ($includeTimer) {
            self::timeFinish();
        }

        self::fold("end", $foldName);

        if ($retVal !== 0) {
            exit($retVal);
        }
    }

    public static function timeStart()
    {
        self::$timerID = sprintf("%08x", rand() * rand());
        self::$timerStart = self::getNanosecondsTime();
        echo "travis_time:start:".self::$timerID."\r".self::ANSI_CLEAR;
    }

    public static function timeFinish()
    {
        $start = self::$timerStart;
        $end = self::getNanosecondsTime();
        $duration = bcsub($end, $start);
        echo "travis_time:end:".self::$timerID.":start=$start,finish=$end,duration=$duration\r".self::ANSI_CLEAR;
    }

    public static function getNanosecondsTime()
    {
        return bcmul(microtime(true), '1000000000');
    }
}
