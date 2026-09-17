# Tests

After Composer installs the development dependencies, run `php test/JobsTest.php`.

`JobsTest.php` covers the enqueue flag, terminal-call headers, and immutable snapshots from both dequeue methods. It uses the real client decoder to check empty objects and arrays, numeric object keys, signed 64-bit integers, floats, escaped strings, gzip responses, and nested worker changes.

On Linux, start a disposable Bedrock server with the `Jobs,DB` plugins, then run:

```sh
BEDROCK_TEST_PORT=18888 php -d auto_prepend_file= -d auto_append_file= test/BedrockWorkerManagerTest.php
```

This starts real worker-manager processes and checks completion, retry, fatal failure, missing workers, and a second run with new data. It also verifies unchanged nested objects do not cause extra runs and legacy empty jobs still finish. The test uses unique job names and deletes its remaining jobs afterward. Set `BEDROCK_TEST_LEGACY=1` to run only the compatibility checks against a Bedrock version without rerun support. The PHP options disable any unrelated application bootstrap configured by the development VM.
