<?php

namespace Expensify\Bedrock\CI;

/**
 * This file is use by our CI system to do a code static analysis with Phan.
 */
class PhanAnalyzer extends CommandLine
{
    /**
     * @var string Branch we are checking (usually coming from['TRAVIS_BRANCH'])
     */
    private $branch;

    /**
     * @param string $branchToCheck
     */
    public function __construct($branchToCheck)
    {
        $this->branch = $branchToCheck;
    }

    /**
     * Analyzes the code.
     *
     * @return bool true if all is good, false if errors were found.
     */
    public function analyze()
    {
        if ($this->branch === 'master') {
            echo 'Skipping style check for merge commits';

            return true;
        }

        $this->checkoutBranch($this->branch);

        Travis::fold("start", "phan.analyze");
        Travis::timeStart();
        echo 'Analyze PHP using Phan'.PHP_EOL;
        $changedFiles = $this->eexec("git diff master...{$this->branch} --name-status | egrep \"^[A|M].*\\.php$\" | cut -f 2");
        echo "Analyzing files:".PHP_EOL;
        $lintErrors = $this->eexec("./vendor/bin/phan -p -z --processes 5", false);
        $lintOK = true;
        foreach ($lintErrors as $lintError) {
            foreach ($changedFiles as $file) {
                if (strpos($lintError, $file) === 0) {
                    echo 'FAIL! '.$file.': '.$lintError.PHP_EOL;
                    $lintOK = false;
                    break;
                }
            }
        }
        Travis::timeFinish();
        Travis::fold("end", "phan.analyze");

        return $lintOK;
    }
}
