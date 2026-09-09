# Tests

After Composer installs the development dependencies, run `php test/JobsTest.php`.

`BedrockWorkerManager.php` starts command-line parsing, Bedrock I/O, load management, and process forking when PHP loads the file. It does not have an existing unit-test seam. `JobsTest.php` therefore checks the terminal-call headers through `Jobs::call`. Full worker-manager forwarding requires an integration test with Bedrock and a worker process.
