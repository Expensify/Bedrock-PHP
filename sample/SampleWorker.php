<?php
/**
 * Sample BedrockWorkerManager worker class.
 */
class SampleWorker extends BedrockWorker
{
    /**
     * This is called once to run the worker. The thread will be forked before
     * this is called, and after this function exits the process will exit.
     */
    public function run()
    {
        $this->bedrock->getLogger()->info("Running SampleWorker for '{$this->job['name']}'");
    }
}
