<?php

use Expensify\Bedrock\BedrockWorkerManager;
use Expensify\Bedrock\Client;

/*
 * BedrockWorkerManager
 * ====================
 * This script runs from the command line and performs the following basic logic:
 *
 * 1. Wait for resources to free up
 * 2. Wait for a job
 * 3. Spawn a worker for that job
 * 4. Goto 1
 * After N cycle in the loop, we exit
 * If the versionWatchFile modified time changes, we stop processing new jobs and exit after finishing all running jobs.
 *
 * Usage: `Usage: sudo -u user php ./bin/BedrockWorkerManager.php --jobName=<jobName> --workerPath=<workerPath> --maxLoad=<maxLoad> [--host=<host> --port=<port> --maxIterations=<loopIteration> --versionWatchFile=<file> --writeConsistency=<consistency>]`
 */

// Verify it's being started correctly
if (php_sapi_name() !== "cli") {
    throw new Exception('This script is cli only');
}
if (!file_exists('/proc/loadavg')) {
    throw new Exception('are you in a chroot?  If so, please make sure /proc is mounted correctly');
}

// Get the options and check we are not missing any of the required ones.
$options = getopt('', ['host::', 'port::', 'maxLoad::', 'maxIterations::', 'jobName::', 'logger::', 'stats::', 'workerPath::', 'versionWatchFile::', 'writeConsistency::']);
$jobName = isset($options['jobName']) ? $options['jobName'] : null;
$maxLoad = isset($options['maxLoad']) && floatval($options['maxLoad']) ? floatval($options['maxLoad']) : 0;
$maxLoopIteration = isset($options['maxIterations']) && intval($options['maxIterations']) ? intval($options['maxIterations']) : 0;
if (!$maxLoopIteration) {
    $maxLoopIteration = 1000;
}
$versionWatchFile = isset($options['versionWatchFile']) ? $options['versionWatchFile'] : null;
$workerPath = isset($options['workerPath']) ? $options['workerPath'] : null;
if (!$jobName || !$maxLoad || !$workerPath) {
    throw new Exception('Usage: sudo -u user php ./bin/BedrockWorkerManager.php --jobName=<jobName> --workerPath=<workerPath> --maxLoad=<maxLoad> [--host=<host> --port=<port> --maxIterations=<loopIteration> --writeConsistency=<consistency>]');
}
if ($maxLoad <= 0) {
    throw new Exception('Maximum load must be greater than zero');
}

// Initialize the bedrock config
$bedrockConfig = [];
if (isset($options['host'])) {
    $bedrockConfig['host'] = $options['host'];
}
if (isset($options['port'])) {
    $bedrockConfig['port'] = $options['port'];
}
if (isset($options['logger'])) {
    $bedrockConfig['logger'] = $options['logger'];
}
if (isset($options['stats'])) {
    $bedrockConfig['stats'] = $options['stats'];
}
if (isset($options['connectionTimeout'])) {
    $bedrockConfig['connectionTimeout'] = $options['connectionTimeout'];
}
if (isset($options['readTimeout'])) {
    $bedrockConfig['readTimeout'] = $options['readTimeout'];
}
if (isset($options['failoverHost'])) {
    $bedrockConfig['failoverHost'] = $options['failoverHost'];
}
if (isset($options['failoverPort'])) {
    $bedrockConfig['failoverPort'] = $options['failoverPort'];
}
if (isset($options['writeConsistency'])) {
    $bedrockConfig['writeConsistency'] = $options['writeConsistency'];
}

// Start processing jobs
$logger = Client::getLogger();
$stats = Client::getStats();
$bwm = new BedrockWorkerManager($versionWatchFile, $maxLoad, $maxLoopIteration, $bedrockConfig);
$bwm->start($workerPath, $jobName);
