# Tests

After Composer installs the development dependencies, run `php test/JobsTest.php`.

`JobsTest.php` covers the enqueue flag, terminal-call headers, and immutable snapshots from both dequeue methods. It uses the real client decoder to check empty objects and arrays, numeric object keys, signed 64-bit integers, floats, escaped strings, gzip responses, and nested worker changes.

`BedrockWorkerManager.php` starts command-line parsing, Bedrock I/O, load management, and process forking when PHP loads the file. Full worker-manager forwarding requires an integration test with Bedrock and a worker process.
