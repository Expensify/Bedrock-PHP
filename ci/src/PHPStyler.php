<?php

namespace Expensify\Bedrock\CI;

/**
 * This file is use by our CI system to make sure the committed files match the Expensify style guide.
 */
class PHPStyler extends CommandLine
{
    /**
     * @var string Branch we are checking (usually coming from['GITHUB_REF'])
     */
    private $branch;

    /**
     * @var string Commit we are checking (usually coming from['GITHUB_SHA'])
     */
    private $commit;

    /**
     * PHPStyler constructor.
     *
     * @param string $branchToCheck
     * @param string $commit
     */
    public function __construct($branchToCheck, $commit)
    {
        $this->branch = $branchToCheck;
        $this->commit = $commit;
    }

    /**
     * Checks the style.
     *
     * @return bool true if all is good, false if errors were found.
     */
    public function check()
    {
        $PHPLintCommand = "find . -name '*.php' -not \\( -path './externalLib/*' -or -path './vendor/*' -or -path './build/*' \\) -print0 | xargs -0 -L 1 -n 1 -P 8 php -l 1>/dev/null";

        if ($this->branch === 'main') {
            Travis::foldCall("lintmaster.php", $PHPLintCommand);

            echo 'Skipping style check for merge commits';

            return true;
        }

        $this->checkoutBranch($this->branch);

        Travis::foldCall("lint.php", $PHPLintCommand);

        Travis::fold("start", "style.php");
        Travis::timeStart();
        echo 'Enforce PHP style'.PHP_EOL;
        $output = $this->getModifiedFiles($this->branch);
        $lintedFiles = [];
        $lintOK = true;
        $dir = $this->eexec('git rev-parse --show-toplevel')[0];
        $gitRepoName = $this->eexec("basename $dir")[0];
        foreach ($output as $file) {
            echo "Linting $file... ".PHP_EOL;

            $fixerCmd = "$dir/ci/php-style-fixer fix --diff $file";
            $fileResult = $this->eexec($fixerCmd, true);
            $fileOK = true;
            foreach ($fileResult as $index => $line) {
                // When a file is fixed, it outputs `   1) File.php` and this is the only way we have to detect if
                // something was fixed or not as the linter only exits with an error exit code when the fixer actually
                // fails (not when it fixes something).
                if (preg_match('/^\W*1\)/', $line)) {
                    $fileOK = false;
                }
            }
            $lintedFiles[] = [$file, $fileOK, join(PHP_EOL, $fileResult)];
            $lintOK = $lintOK && $fileOK;
        }

        echo "\n\nLinted files:\n\n";

        foreach ($lintedFiles as $lint) {
            echo $lint[0].'... '.($lint[1] ? 'OK!' : 'FAIL!'.PHP_EOL.$lint[2]).PHP_EOL;
        }

        Travis::timeFinish();
        Travis::fold("end", "style.php");

        if (!$lintOK) {
            return false;
        }

        Travis::foldCall("git.checkout2", "git checkout {$this->commit} 2>&1");

        return true;
    }
}
