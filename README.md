# Bedrock-PHP
This is a library to interact with [Bedrock](https://github.com/Expensify/Bedrock)

# Raw Responses

JSON response bodies are decoded by default. To inspect the status and headers first, request the untouched response body:

```php
$response = $client->call('OpenApp', $headers, shouldDecodeResponse: false);

// Decode only if the caller needs to inspect or process the body.
$decodedResponse = $client->decodeResponse($response);
```

Raw mode preserves the normal response metadata (`code`, `codeLine`, `headers`, and `size`) and returns `body` as a string without JSON parsing or repair. Compressed bodies remain compressed until `decodeResponse()` performs the normal decompression and JSON parsing. Non-JSON responses and already-decoded responses pass through `decodeResponse()` unchanged. Raw bodies may contain credentials and must not be logged.

Run the focused transport and decoder checks with PHP's sockets, pcntl, and posix extensions:

```bash
php -d auto_prepend_file= -d auto_append_file= tests/ClientRawResponseTest.php
```

# Publishing Your Changes
Merging a pull request into `main` publishes a new version. A workflow tags the
merge commit with the next patch version and comments that version on the pull
request. There is nothing to tag by hand.

`composer.json` has no `version` field on purpose. Composer resolves this
library to the version declared there in preference to the tag name, so the tag
is the only place a version is recorded.

To pick up a new version, update the `expensify/bedrock-php` constraint in the
consuming repository, such as Web-Expensify or Web-Secure, and run
`composer update expensify/bedrock-php`.

# Tests

With Composer dependencies installed and the APCu extension available, run:

```bash
php -d apc.enable_cli=1 vendor/bin/phpunit
```

PHPUnit discovers all tests in `tests/` through `phpunit.xml`. CI runs the same
suite with APCu enabled.
