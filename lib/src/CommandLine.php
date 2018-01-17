<?php

namespace Expensify\BedrockLibs;

/**
 * This file contains some common methods for scripts that run in the command line.
 */
class CommandLine
{
    /**
     * Checks out a branch and fetches master
     *
     * @param string $branch
     */
    protected function checkoutBranch($branch)
    {
        Travis::fold("start", "git.checkout.".get_called_class());
        $this->call("git checkout $branch 2>&1");
        $this->call('git fetch origin master:master 2>&1');
        Travis::fold("end", "git.checkout.".get_called_class());
    }

    /**
     * Get the new/modified PHP files in a branch, with respect to master.
     *
     * @param string $branch
     *
     * @return array
     */
    protected function getModifiedFiles($branch)
    {
        return $this->eexec("git diff master...$branch --name-status | grep -v 'vendor/' | egrep \"^[A|M].*\\.php$\" | cut -f 2");
    }

    /**
     * @param string $cmd
     * @param bool   $exitOnFail
     *
     * @return bool
     */
    protected function call($cmd, $exitOnFail = true)
    {
        echo $cmd.PHP_EOL;
        system($cmd, $ret);
        if ($ret !== 0) {
            if ($exitOnFail) {
                echo 'Error: '.$ret.PHP_EOL;
                exit($ret);
            }

            return false;
        }

        return true;
    }

    /**
     * @param string $cmd
     * @param bool   $exitOnFail
     *
     * @return array
     */
    protected function eexec($cmd, $exitOnFail = true)
    {
        echo $cmd.PHP_EOL;
        exec($cmd, $output, $ret);
        if ($ret !== 0 && $exitOnFail) {
            echo 'Error: '.$ret.PHP_EOL;
            exit($ret);
        }

        return $output;
    }
}
