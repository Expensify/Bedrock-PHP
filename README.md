# Bedrock-PHP
This is a library to interact with [Bedrock](https://github.com/Expensify/Bedrock)

# Request names

`Client::call()` rejects empty method and header names, colons, and control characters with an `InvalidArgumentException` before sending any bytes. Header names cannot contain spaces. Indexed headers such as `amount[0]` and HTTP-style methods such as `GET /status/handlingCommands HTTP/1.1` are supported.

# Raw Responses

JSON response bodies are decoded by default. To leave eligible successful bodies as strings, disable decoding:

```php
$response = $client->call('OpenApp', $headers, shouldDecodeResponse: false);
```

Raw mode preserves the response metadata (`code`, `codeLine`, `headers`, and `size`) and leaves successful bodies as strings without JSON parsing or repair. Responses outside the recognized success statuses 200 and 202, responses with `X-Session-Refresh-Required`, and gzip responses use the normal decoder even when `shouldDecodeResponse` is false. Callers must distinguish a raw string body from a decoded body.

# Command Timeouts

A command that times out (`555 Timeout`) throws `TimeoutError`. By default the client does not retry it or blacklist the host, because the command would likely time out on every host.

A health check can instead treat a host that is too slow to answer as unavailable:

```php
$client->call('GetRestrictedAuthToken', ['timeout' => 3000, 'idempotent' => true, 'shouldBlacklistHostOnTimeout' => true]);
```

With `shouldBlacklistHostOnTimeout` (default false), the client blacklists the host that timed out and still throws `TimeoutError`, so the caller's next request goes to another host. The header is not sent to the server.

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
