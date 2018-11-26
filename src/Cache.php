<?php

namespace Expensify\Bedrock;

use Expensify\Bedrock\Exceptions\ConnectionFailure;
use Expensify\Bedrock\Exceptions\Jobs\DoesNotExist;

/**
 * Encapsulates the built-in Cache plugin to Bedrock.
 *
 * @see https://github.com/Expensify/Bedrock/blob/master/plugins/Cache.md
 */
class Cache extends Plugin
{
    /**
     * Reads a named value from the cache.  Can optionally request a specific
     * version of that value, if available.
     *
     * @param string $name    Name pattern (using LIKE syntax) to read.
     * @param string $version (optional) Specific version identifier (ie, a timestamp, counter, name, etc), defaults to the latest
     *
     * @return mixed Whatever was saved in the cache
     *
     * @throws DoesNotExist
     */
    public function read($name, $version = null)
    {
        $fullName = ($version ? "$name/$version" : "$name/*");
        $this->client->getLogger()->info("BedrockCache read", [
            'key' => $name,
            'version' => $version,
        ]);
        $response = $this->call("ReadCache", ["name" => $fullName]);
        if ($response['code'] === 404) {
            throw new DoesNotExist('The cache entry could not be found', 666);
        }
        return $response['body'];
    }

    /**
     * Reads from the cache, but if it does not find the entry, it returns the passed default.
     *
     * @param string $name
     * @param mixed  $default
     * @param string $version
     *
     * @return mixed
     */
    public function readWithDefault($name, $default, $version = null)
    {
        try {
            return $this->read($name, $version);
        } catch (DoesNotExist $e) {
            return $default;
        }
    }

    /**
     * Gets data from a cache, if it is not present, it computes it by calling $computeFunction and saves the result in the cache.
     *
     * @param string      $name
     * @param null|string $version
     * @param callable    $computeFunction
     *
     * @return array
     */
    public function get(string $name, ?string $version, callable $computeFunction)
    {
        try {
            return $this->read($name, $version);
        } catch (DoesNotExist $e) {
            $value = $computeFunction();
            $this->write($name, $value, $version);
            return $value;
        }
    }

    /**
     * Writes a named value to the cache, overriding any value of the same
     * name.  If a version is provided, also invalidates all other versions of
     * the value.  This write is asynchronous (eg, it returns when it has been
     * successfully queued with the server, but before the write itself has
     * completed).
     *
     * @param string $name    Arbitrary string used to uniquely name this value.
     * @param mixed  $value   Raw binary data to associate with this name
     * @param string $version (optional) Version identifier (eg, a timestamp, counter, name, etc)
     */
    public function write($name, $value, $version = null)
    {
        // If we have a version, invalidate previous versions
        $headers = [
            "Connection" => "forget",
        ];
        if ($version) {
            // Invalidate all other versions of this name before setting
            $headers["invalidateName"] = "$name/*";
            $headers["name"] = "$name/$version";
        } else {
            // Just set this name
            $headers["name"] = "$name/";
        }

        $this->call("WriteCache", $headers, json_encode($value));
    }

    /**
     * Call the bedrock cache methods, and handle connection error.
     *
     * @param string $body
     *
     * @return mixed|null
     */
    private function call(string $method, array $headers, $body = '')
    {
        // Both writing to and reading from the cache are always idempotent operations
        $headers['idempotent'] = true;

        try {
            return $this->client->getStats()->benchmark("bedrock.cache.$method", function () use ($method, $headers, $body) {
                return $this->client->call($method, $headers, $body);
            });
        } catch (ConnectionFailure $e) {
            $this->client->getLogger()->alert('Bedrock Cache failure', ['exception' => $e]);
        }
    }
}
