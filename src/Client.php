<?php

namespace Expensify\Bedrock;

use Expensify\Bedrock\Exceptions\BedrockError;
use Expensify\Bedrock\Exceptions\ConnectionFailure;
use Expensify\Bedrock\Stats\NullStats;
use Expensify\Bedrock\Stats\StatsInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Socket;

/**
 * Client for communicating with bedrock.
 */
class Client implements LoggerAwareInterface
{
    public const HEADER_DELIMITER = "\r\n\r\n";
    public const HEADER_FIELD_SEPARATOR = "\r\n";

    /**
     * The length of each packet read from the socket.
     */
    public const PACKET_LENGTH = 16384;

    /**
     * The prefix to use in apcu to store the state of the hosts.
     */
    public const APCU_CACHE_PREFIX = 'bedrockHostConfigs-';

    /**
     * Priorities a command can have.
     */
    public const PRIORITY_MIN = 0;
    public const PRIORITY_LOW = 250;
    public const PRIORITY_NORMAL = 500;
    public const PRIORITY_HIGH = 750;
    public const PRIORITY_MAX = 1000;

    /**
     * @var array This is a default configuration applied to all instances of this class. They can be overriden in the
     *            constructor.
     */
    private static $defaultConfig = [];

    /**
     * @var Client[] An array of all instances of this class that were created, one per different config passed.
     */
    private static $instances = [];

    /**
     * @var int[] An array with the keys being the name of a cluster and the value a commitCount. This variable is set
     *            in clearInstancesAfterFork and can be used to force a specific commitCount to be used when the
     *            instance for that cluster is initialized.
     */
    private static $preloadedCommitCounts = [];

    /**
     *  @var ?int The last commit count of the node we talked to. This is used to ensure if we make a subsequent
     *            request to a different node in the same session, that the node waits until it is at least
     *            up to date with the commits as the node we originally queried.
     */
    public $commitCount = null;

    /**
     *  @var resource|Socket|null Socket to the server.
     */
    private $socket = null;

    /**
     * @var string|null Name of the bedrock cluster we are talking to. If you have more than one bedrock cluster, you
     *                  can pass in different names for them in order to have separate statistics collected and caches of failed servers.
     */
    private $clusterName = null;

    /**
     *  @var array List of hosts to use as first choice. It will pick just one of these randomly and try it first.
     */
    private $mainHostConfigs = [];

    /**
     *  @var array List of failovers we attempt if the first didn't work. We randomize the list and try on several of
     *             them (depending on the number of retries configured).
     */
    private $failoverHostConfigs = [];

    /**
     * @var int Timeout for connecting to the server.
     */
    private $connectionTimeout;

    /**
     * @var int Timeout for reading the response from the server.
     */
    private $readTimeout;

    /**
     * @var int Timeout to pass to bedrock.
     */
    private $bedrockTimeout;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var StatsInterface
     */
    private $stats;

    /**
     * @var string The bedrock write consistency we want to use.
     */
    private $writeConsistency;

    /**
     * @var int When a host fails, it will blacklist it and not try to reuse it for up to this amount of seconds.
     */
    private $maxBlackListTimeout;

    /**
     * @var bool Set this to true to add a `mockRequest` header to all outgoing requests.
     */
    private $mockRequests;

    /**
     * @var string The last host we successfully used.
     */
    private $lastHost = '';

    /**
     * @var int The priority to send the commands with.
     */
    private $commandPriority;

    /**
     * @var ?string Extra data to add to the bedrock logs
     */
    private $logParam;

    /**
     * Creates a reusable Bedrock instance.
     * All params are optional and values set in `configure` would be used if are not passed here.
     *
     * @param array $config Configuration to use, can have all of these:
     *                      string               clusterName         Name of the bedrock cluster. This is used to separate requests made to different bedrock clusters.
     *                      array|null           mainHostConfigs     List of hosts to attempt first
     *                      array|null           failovers           List of hosts to use as failovers
     *                      int|null             connectionTimeout   Timeout to use when connecting
     *                      int|null             readTimeout         Timeout to use when reading
     *                      int|null             bedrockTimeout      Timeout to use for bedrock commands
     *                      LoggerInterface|null logger              Class to use for logging
     *                      StatsInterface|null  stats               Class to use for statistics tracking
     *                      string|null          writeConsistency    The bedrock write consistency we want to use
     *                      int|null             maxBlackListTimeout When a host fails, it will blacklist it and not try to reuse it for up to this amount of seconds.
     *                      int|null             commandPriority     The priority to send the commands with
     *                      string|null          logParam            Extra data to add to the bedrock logs
     *
     * @throws BedrockError
     */
    private function __construct(array $config = [])
    {
        $config = array_merge(self::$defaultConfig, $config);
        $this->clusterName = $config['clusterName'];
        $this->mainHostConfigs = $config['mainHostConfigs'];
        $this->failoverHostConfigs = $config['failoverHostConfigs'];
        $this->connectionTimeout = $config['connectionTimeout'];
        $this->readTimeout = $config['readTimeout'];
        $this->bedrockTimeout = $config['bedrockTimeout'];
        $this->logger = $config['logger'];
        $this->stats = $config['stats'];
        $this->writeConsistency = $config['writeConsistency'];
        $this->maxBlackListTimeout = $config['maxBlackListTimeout'];
        $this->commandPriority = $config['commandPriority'];
        $this->logParam = $config['logParam'];

        // If the caller explicitly set `mockRequests`, use that value.
        if (isset($config['mockRequests'])) {
            $this->mockRequests = $config['mockRequests'];
        } elseif (isset($_SERVER['HTTP_X_MOCK_REQUEST'])) {
            // otherwise check the http headers and set it
            $this->mockRequests = isset($_SERVER['HTTP_X_MOCK_REQUEST']);
        }

        // Make sure we have at least one host configured
        $this->logger->debug('Bedrock\Client - Constructed', ['clusterName' => $this->clusterName, 'mainHostConfigs' => $this->mainHostConfigs, 'failoverHostConfigs' => $this->failoverHostConfigs]);
        if (empty($this->mainHostConfigs)) {
            throw new BedrockError('Main hosts are not set, cannot instantiate bedrock client');
        }
    }

    /**
     * Returns an instance of this class for the specified configuration. It will return the same instance if the same
     * configuration was passed (not counting the logger and stats params), unless clearInstancesAfterFork is called.
     */
    public static function getInstance(array $config = []): Client
    {
        $config = array_merge(self::$defaultConfig, $config);
        ksort($config);
        $configForHash = $config;
        unset($configForHash['logger']);
        unset($configForHash['stats']);
        unset($configForHash['logParam']);
        $hash = sha1(print_r($configForHash, true));
        if (isset(self::$instances[$hash])) {
            return self::$instances[$hash];
        }
        $instance = new self($config);

        // If we had a preloaded commitCount, set it so that the first request uses it.
        if (isset(self::$preloadedCommitCounts[$instance->getClusterName()])) {
            $instance->commitCount = self::$preloadedCommitCounts[$instance->getClusterName()];
        }
        self::$instances[$hash] = $instance;

        return $instance;
    }

    /**
     * After forking, you need to call this method to make sure the forks don't share the same socket and instead open
     * a new connection.
     *
     * @param int[] $preloadedCommitCounts
     */
    public static function clearInstancesAfterFork(array $preloadedCommitCounts)
    {
        self::$instances = [];
        self::$preloadedCommitCounts = $preloadedCommitCounts;
    }

    /**
     * Sets the default config to use, these are used as defaults each time you create a new instance.
     */
    public static function configure(array $config)
    {
        // Store the configuration
        self::$defaultConfig = array_merge([
            'clusterName' => 'bedrock',
            'mainHostConfigs' => ['localhost' => ['blacklistedUntil' => 0, 'port' => 8888]],
            'failoverHostConfigs' => ['localhost' => ['blacklistedUntil' => 0, 'port' => 8888]],
            'connectionTimeout' => 1,
            'readTimeout' => 120,
            'bedrockTimeout' => 110,
            'logger' => new NullLogger(),
            'stats' => new NullStats(),
            'writeConsistency' => 'ASYNC',
            'maxBlackListTimeout' => 1,
            'commandPriority' => null,
            'logParam' => null,
        ], self::$defaultConfig, $config);
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger()
    {
        return $this->logger;
    }

    /**
     * Sets a logger instance on the object.
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;

        return null;
    }

    /**
     * @return StatsInterface
     */
    public function getStats()
    {
        if (is_string($this->stats)) {
            $this->stats = new $this->stats();
        }

        return $this->stats;
    }

    /**
     * Returns the last host successfully used.
     */
    public function getLastHost(): string
    {
        return $this->lastHost;
    }

    /**
     * Returns the cluster name.
     */
    public function getClusterName(): string
    {
        return $this->clusterName;
    }

    /**
     * Makes a direct call to Bedrock.
     *
     * @param string $method  Request method
     * @param array  $headers Request headers (optional)
     * @param string $body    Request body (optional)
     *
     * @return array JSON response, or null on error
     *
     * @throws BedrockError
     * @throws ConnectionFailure
     */
    public function call($method, $headers = [], $body = '')
    {
        // Start timing the entire end-to-end
        $timeStart = microtime(true);

        // Include the last CommitCount, if we have one
        if ($this->commitCount) {
            $headers['commitCount'] = $this->commitCount;
        }

        // Include the requestID for logging purposes
        if (isset($GLOBALS['REQUEST_ID'])) {
            $headers['requestID'] = $GLOBALS['REQUEST_ID'];
        }
        $headers['lastIP'] = $_SERVER['HTTP_X_FORWARDED_FOR'] ?? $_SERVER['REMOTE_ADDR'] ?? null;

        // Set the write consistency
        if ($this->writeConsistency) {
            $headers['writeConsistency'] = $this->writeConsistency;
        }

        // Add mock request header if set.
        if ($this->mockRequests) {
            $headers['mockRequest'] = true;
        }

        if ($this->commandPriority) {
            $headers['priority'] = $this->commandPriority;
        }

        if (!array_key_exists('timeout', $headers) && $this->bedrockTimeout) {
            $headers['timeout'] = $this->bedrockTimeout * 1000;
        }

        if (!array_key_exists('logParam', $headers) && $this->logParam) {
            $headers['logParam'] = $this->logParam;
        }

        $this->logger->info('Bedrock\Client - Starting a request', [
            'command' => $method,
            'clusterName' => $this->clusterName,
            'headers' => $headers,
        ]);

        // Construct the request
        $rawRequest = "$method\r\n";
        foreach ($headers as $name => $value) {
            if (is_array($value) || is_object($value)) {
                // Passing flag JSON_PRESERVE_ZERO_FRACTION because otherwise PHP will serialize floating numbers like 2.0 to 2 instead of 2.0.
                // This can cause bugs in Auth since we can't know what type a float number will be and accessing an int with the accessor for
                // floats (or vice versa) would throw.
                $rawRequest .= "$name: ".addcslashes(json_encode($value, JSON_PRESERVE_ZERO_FRACTION), '\\')."\r\n";
            } elseif (is_bool($value)) {
                $rawRequest .= "$name: ".($value ? 'true' : 'false')."\r\n";
            } elseif ($value === null || $value === '') {
                // skip empty values
            } else {
                $rawRequest .= "$name: ".self::toUTF8(addcslashes($value, "\r\n\t\\"))."\r\n";
            }
        }
        $rawRequest .= 'Content-Length: '.strlen($body)."\r\n";
        $rawRequest .= "\r\n";
        $rawRequest .= $body;

        $response = null;
        $preferredHost = null;
        if (isset($headers['host'])) {
            $preferredHost = $headers['host'];
            unset($headers['host']);
        }
        $hostConfigs = $this->getPossibleHosts($preferredHost);

        // If we passed a preferred host and we already had a connected socket, but to a different host and the preferred
        // host is not blacklisted (the preferred host is returned first in the possible hosts array only when it's not blacklisted)
        // then we close the socket in order to connect to the preferred one.
        $closeSocketAfterRequest = array_key_exists('Connection', $headers) ? $headers['Connection'] === 'close' : false;

        if ($preferredHost && $this->socket && key($hostConfigs) !== $this->lastHost) {
            @socket_close($this->socket);
            $this->socket = null;
            $closeSocketAfterRequest = true;
        }

        $hostName = null;
        $retriedAllHosts = false;
        while (!$response && count($hostConfigs)) {
            $exception = null;
            reset($hostConfigs);
            $numRetriesLeft = count($hostConfigs) - 1;

            if ($this->lastHost && $this->socket && !isset($hostConfigs[$this->lastHost])) {
                // If we have a socket connection, but the current host is no longer in the list of available host
                // configs, close the socket so it can be reset.
                @socket_close($this->socket);
                $this->socket = null;
                $this->logger->info('Bedrock\Client - Not reusing socket because the host it was connected to is no longer available', ['host' => $this->lastHost]);
            }
            // If we already have a socket for this instance, then we first try to reuse it
            if ($this->socket) {
                $hostName = $this->lastHost;
            } else {
                // Try the first possible host.
                $hostName = (string) key($hostConfigs);
                $this->lastHost = $hostName;
            }
            try {
                // We get the port from either the main or failover host configs, due to socket reuse, the host we are
                // trying to use might not be in the picked host configs, because getPossibleHosts randomizes them.
                $port = $this->mainHostConfigs[$hostName]['port'] ?? $this->failoverHostConfigs[$hostName]['port'];
                $this->sendRawRequest($hostName, $port, $rawRequest);
                $response = $this->receiveResponse();
            } catch (ConnectionFailure $e) {
                // The error happened during connection (or before we sent any data, or in a case where we know the
                // command was never processed) so we can retry it safely.
                $this->markHostAsFailed($hostName);
                if ($numRetriesLeft) {
                    $this->logger->info('Bedrock\Client - Failed to connect or send the request; retrying', ['host' => $hostName, 'message' => $e->getMessage(), 'retriesLeft' => $numRetriesLeft, 'exception' => $e]);
                } else {
                    if ($retriedAllHosts) {
                        $this->logger->error('Bedrock\Client - Failed to connect or send the request; not retrying because we are out of retries', ['host' => $hostName, 'message' => $e->getMessage(), 'exception' => $e]);
                    } else {
                        $this->logger->info('Bedrock\Client - Failed to connect or send the request; retrying in all hosts', ['host' => $hostName, 'message' => $e->getMessage(), 'exception' => $e]);
                    }
                    $exception = $e;
                }
            } catch (BedrockError $e) {
                // This error happen after sending some data to the server, so we only can retry it if it is an idempotent command
                $this->markHostAsFailed($hostName);
                /* @phan-suppress-next-line PhanTypeInvalidDimOffset for some reason phan says idempotent does not exist, but I have the ?? so it should not matter */
                if (!($headers['idempotent'] ?? false)) {
                    $this->logger->error('Bedrock\Client - Failed to send the whole request or to receive it; not retrying because command is not idempotent', ['host' => $hostName, 'message' => $e->getMessage(), 'exception' => $e]);
                    throw $e;
                }
                if ($numRetriesLeft) {
                    $this->logger->info('Bedrock\Client - Failed to send the whole request or to receive it; retrying because command is idempotent', ['host' => $hostName, 'message' => $e->getMessage(), 'retriesLeft' => $numRetriesLeft, 'exception' => $e]);
                } else {
                    if ($retriedAllHosts) {
                        $this->logger->error('Bedrock\Client - Failed to send the whole request or to receive it; not retrying because we are out of retries', ['host' => $hostName, 'message' => $e->getMessage(), 'exception' => $e]);
                    } else {
                        $this->logger->info('Bedrock\Client - Failed to send the whole request or to receive it; retrying in all hosts', ['host' => $hostName, 'message' => $e->getMessage(), 'exception' => $e]);
                    }
                    $exception = $e;
                }
            } finally {
                // We remove the host we just used from the possible hosts to use, in case we are retrying
                $hostConfigs = array_filter($hostConfigs, function ($possibleHost) use ($hostName) {
                    return $possibleHost !== $hostName;
                }, ARRAY_FILTER_USE_KEY);
            }

            // All non blacklisted hosts failed, this could be because we are in the middle of a cluster version flip.
            // ie: We have version 1 and version 2, we've installed version 2 in less than half the cluster, a previous
            // request already marked all servers in version 2 as failed (since 1 is the current version) in this request
            // we have installed version 2 in one more server, making it the current version. So now all the servers that
            // were marked as failed in the previous request are the ones serving requests and all the ones that were good
            // before are now in the old version and not serving requests. So to cover this, we retry in all servers
            // once hoping it will find a server that works.
            if ($exception) {
                if ($retriedAllHosts) {
                    throw $exception;
                }
                $retriedAllHosts = true;
                $this->logger->info('All non blacklisted hosts failed, as a last resort try again in all hosts');
                $hostConfigs = $this->getPossibleHosts($preferredHost, true);
            }
        }

        if (is_null($response)) {
            throw new ConnectionFailure('Could not connect to Bedrock hosts or failovers');
        }

        // If we had to close the socket after using it (because we connected to a preferred host or because the command
        // had Connection:forget), disconnect from it so we don't send all future requests to it.
        if ($closeSocketAfterRequest || ($response['headers']['Connection'] ?? '') === 'close') {
            $this->logger->info('Closing socket after use');
            @socket_close($this->socket);
            $this->socket = null;
        }

        // Log how long this particular call took
        $processingTime = (isset($response['headers']['processTime']) ? $response['headers']['processTime'] : 0) / 1000;
        $serverTime = (isset($response['headers']['totalTime']) ? $response['headers']['totalTime'] : 0) / 1000;
        $clientTime = round(microtime(true) - $timeStart, 3) * 1000;
        $networkTime = $clientTime - $serverTime;
        $waitTime = $serverTime - $processingTime;
        $this->logger->info('Bedrock\Client - Request finished', [
            'host' => $hostName,
            'command' => $method,
            'jsonCode' => isset($response['codeLine']) ? $response['codeLine'] : null,
            'duration' => $clientTime,
            'net' => $networkTime,
            'wait' => $waitTime,
            'proc' => $processingTime,
            'commitCount' => $this->commitCount,
        ]);

        // Done!
        return $response;
    }

    /**
     * Sends the request on a new socket, if a previous one existed, it closes the connection first.
     *
     * @throws ConnectionFailure When the failure is before sending any data to the server
     * @throws BedrockError      When we already sent some data
     */
    private function sendRawRequest(string $host, int $port, string $rawRequest)
    {
        // Try to connect to the requested host
        $pid = getmypid();
        if (!$this->socket) {
            $this->logger->info('Bedrock\Client - Opening new socket', ['host' => $host, 'cluster' => $this->clusterName, 'pid' => $pid]);
            $this->socket = @socket_create(AF_INET, SOCK_STREAM, getprotobyname('tcp'));

            // Make sure we succeed to create a socket
            if ($this->socket === false) {
                $socketError = socket_strerror(socket_last_error());
                throw new ConnectionFailure("Could not connect to create socket: $socketError");
            }

            // Configure this socket and try to connect to it
            socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => $this->connectionTimeout, 'usec' => 0]);
            socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $this->readTimeout, 'usec' => 0]);
            @socket_connect($this->socket, $host, $port);
            $socketErrorCode = socket_last_error($this->socket);
            if ($socketErrorCode === 115) {
                $this->logger->info('Bedrock\Client - socket_connect returned error 115, continuing.');
            } elseif ($socketErrorCode) {
                $socketError = socket_strerror($socketErrorCode);
                throw new ConnectionFailure("Could not connect to Bedrock host $host:$port. Error: $socketErrorCode $socketError");
            }
        } else {
            $this->logger->info('Bedrock\Client - Reusing socket', ['host' => $host, 'cluster' => $this->clusterName, 'pid' => $pid]);
        }
        socket_clear_error($this->socket);

        // Send the information to the socket
        $bytesSent = @socket_send($this->socket, $rawRequest, strlen($rawRequest), MSG_EOF);

        // Failed to send anything
        if ($bytesSent === false) {
            $socketErrorCode = socket_last_error();
            $socketError = socket_strerror($socketErrorCode);
            throw new ConnectionFailure("Failed to send request to bedrock host $host:$port. Error: $socketErrorCode $socketError");
        }

        // We sent something; can't retry or else we might double-send the same request. Let's make sure we sent the
        // whole thing, else there's a problem.
        if ($bytesSent < strlen($rawRequest)) {
            $this->logger->info('Bedrock\Client - Could not send the whole request', ['bytesSent' => $bytesSent, 'expected' => strlen($rawRequest)]);
            throw new ConnectionFailure("Sent partial request to bedrock host $host:$port");
        } elseif ($bytesSent > strlen($rawRequest)) {
            $this->logger->info('Bedrock\Client - sent more data than needed', ['bytesSent' => $bytesSent, 'expected' => strlen($rawRequest)]);
            throw new BedrockError("Sent more content than expected to host $host:$port");
        }
    }

    /**
     * @param ?string $preferredHost If passed, it will prefer this host over any of the configured ones. This does not
     *                               ensure it will use that host, but it will try to use it if its not blacklisted.
     * @suppress PhanUndeclaredConstant - suppresses TRAVIS_RUNNING
     */
    private function getPossibleHosts(?string $preferredHost, bool $resetHosts = false)
    {
        // We get the host configs from the APC cache. Then, we check the configuration there with the passed
        // configuration and if it's outdated (ie: it has different hosts from the one in the config), we reset it. This
        // is so that we don't keep the old cache after changing the hosts or failover configuration.
        if ((!defined('TRAVIS_RUNNING') || !TRAVIS_RUNNING)) {
            $apcuKey = self::APCU_CACHE_PREFIX.$this->clusterName;
            if ($resetHosts) {
                $this->logger->info('Bedrock\Client - Resetting host configs');
                $cachedHostConfigs = [];
            } else {
                $cachedHostConfigs = apcu_fetch($apcuKey) ?: [];
            }
            $this->logger->info('Bedrock\Client - APC fetch host configs', array_keys($cachedHostConfigs));

            // If the hosts and ports in the cache don't match the ones in the config, reset the cache.
            $cachedHostsAndPorts = [];
            foreach ($cachedHostConfigs as $hostName => $config) {
                $cachedHostsAndPorts[$hostName] = $config['port'] ?? null;
            }
            asort($cachedHostsAndPorts);
            $uncachedHostsAndPort = [];
            foreach (array_merge($this->mainHostConfigs, $this->failoverHostConfigs) as $hostName => $config) {
                $uncachedHostsAndPort[$hostName] = $config['port'];
            }
            asort($uncachedHostsAndPort);
            if ($cachedHostsAndPorts !== $uncachedHostsAndPort) {
                $cachedHostConfigs = array_merge($this->mainHostConfigs, $this->failoverHostConfigs);
                $this->logger->info('Bedrock\Client - APC store host configs', array_keys($cachedHostConfigs));
                apcu_store($apcuKey, $cachedHostConfigs);
            }
        } else {
            $cachedHostConfigs = array_merge($this->mainHostConfigs, $this->failoverHostConfigs);
        }

        // Get one main host and all the failovers, then remove any of them that we know already failed.
        // Assemble the list of servers we'll try, in order.  First, pick one of the main hosts. We pick randomly
        // because we want to equally balance each server across all of its local databases. This allows us to have an
        // unequal number of servers and databases in a given datacenter. Also, we only pick one (versus trying both)
        // because if our first attempt fails we want to equally balance across *all* databases -- including the remote
        // ones. Otherwise if a database node goes down, the other databases in the same datacenter would get more load
        // (whereas this approach ensures the load is spread evenly across all).
        $failoverHostNames = array_keys($this->failoverHostConfigs);
        shuffle($failoverHostNames);
        $mainHostName = array_rand($this->mainHostConfigs);
        $preferredHost = array_key_exists((string) $preferredHost, $cachedHostConfigs) ? [$preferredHost] : [];
        $hostNames = array_filter(array_unique(array_merge($preferredHost, [$mainHostName], $failoverHostNames)));

        $nonBlackListedHosts = [];
        foreach ($hostNames as $hostName) {
            $blackListedUntil = $cachedHostConfigs[$hostName]['blacklistedUntil'] ?? null;
            if (!$blackListedUntil || $blackListedUntil < time()) {
                $nonBlackListedHosts[$hostName] = $cachedHostConfigs[$hostName];
            }
        }

        if (empty($nonBlackListedHosts)) {
            $this->getLogger()->info('Bedrock\Client - All possible hosts have been blacklisted, using full list instead');
            $nonBlackListedHosts = $cachedHostConfigs;
        }
        $this->getLogger()->info('Bedrock\Client - Possible hosts', ['nonBlacklistedHosts' => array_keys($nonBlackListedHosts)]);

        return $nonBlackListedHosts;
    }

    /**
     * Receives and parses the response.
     *
     * @return array Response object including 'code', 'codeLine', 'headers', `size` and 'body'
     *
     * @throws BedrockError
     */
    private function receiveResponse()
    {
        // Make sure bedrock is returning something https://github.com/Expensify/Expensify/issues/11010
        if (@socket_recv($this->socket, $buf, self::PACKET_LENGTH, MSG_PEEK) === false) {
            throw new BedrockError('Socket failed to read data');
        }

        $totalDataReceived = 0;
        $responseHeaders = [];
        $responseLength = null;
        $response = '';
        $dataOnSocket = '';
        $codeLine = null;

        // Read the data on the socket block by block until we got them all
        do {
            $sizeDataOnSocket = @socket_recv($this->socket, $dataOnSocket, self::PACKET_LENGTH, 0);
            if ($sizeDataOnSocket === false) {
                $errorCode = socket_last_error($this->socket);
                $errorMsg = socket_strerror($errorCode);
                throw new BedrockError("Error receiving data: $errorCode - $errorMsg");
            }
            if ($sizeDataOnSocket === 0 || strlen($dataOnSocket) === 0) {
                throw new BedrockError('Bedrock response was empty');
            }
            $totalDataReceived += $sizeDataOnSocket;
            $response .= $dataOnSocket;

            // The first time are reading data from the socket, we need to extract the headers
            // to be able to get the size of the response
            // It is use to know when to stop to read data from the socket
            if ($responseLength === null && strpos($response, self::HEADER_DELIMITER) !== false) {
                $dataOffset = strpos($response, self::HEADER_DELIMITER);
                $responseHeadersStr = substr($response, 0, $dataOffset + strlen(self::HEADER_DELIMITER));
                $responseHeaderLines = explode(self::HEADER_FIELD_SEPARATOR, $responseHeadersStr);
                $codeLine = array_shift($responseHeaderLines);
                $responseHeaders = $this->extractResponseHeaders($responseHeaderLines);
                $responseLength = (int) $responseHeaders['Content-Length'];
                $response = substr($response, $dataOffset + strlen(self::HEADER_DELIMITER));
            }
        } while (is_null($responseLength) || strlen($response) < $responseLength);

        // We save the commitCount for future requests. This is useful if for some reason we change the bedrock node we
        // are talking to.
        // We only set it if process time was returned, which means we did a write. We don't care about saving the commit
        // count for reads, since we did not change anything in the DB.
        if (isset($responseHeaders['commitCount']) && (($responseHeaders['processTime'] ?? 0) > 0 || ($responseHeaders['upstreamProcessTime'] ?? 0) > 0)) {
            $this->commitCount = (int) $responseHeaders['commitCount'];
        }

        // We treat this '555 Timeout', which is a command timeout (not a query timeout), as a ConnectionFailure so that it gets retried regardless of if it is idempotent or not.
        // For other exceptions that have different error codes/messages, we do not throw here, so it gets handled like any regular exception.
        if ($codeLine === '555 Timeout') {
            throw new ConnectionFailure('Internal Bedrock command timeout (555 Timeout)');
        }

        if ($codeLine === '500 Internal Server Error') {
            throw new ConnectionFailure('Bedrock responded with 500 Internal Server Error');
        }

        // We'll parse the body *only* if this is `application/json` or blank.
        $isJSON = !isset($responseHeaders['Content-Type']) || !strcasecmp($responseHeaders['Content-Type'], 'application/json');

        return [
            'headers' => $responseHeaders,
            'body' => $isJSON ? $this->parseRawBody($responseHeaders, $response) : $response,
            'size' => $totalDataReceived,
            'codeLine' => $codeLine,
            'code' => intval($codeLine),
        ];
    }

    /**
     * Parse a raw response from bedrock.
     *
     * @return array|null the decoded json, or null on error
     *
     * @throws BedrockError
     */
    private function parseRawBody(array $headers, string $body)
    {
        // Detect if we are using Gzip (TODO: can we remove this?)
        if (isset($headers['Content-Encoding']) && $headers['Content-Encoding'] === 'gzip') {
            $body = gzdecode($body);
            if ($body === false) {
                throw new BedrockError('Could not gzip decode bedrock response');
            }
        } else {
            // Who knows why we need to trim in this case?
            $body = trim($body);
        }

        if ($body === '') {
            return [];
        }

        $json = json_decode($body, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            // This will remove unwanted characters.
            // Check http://stackoverflow.com/a/20845642 and http://www.php.net/chr for details
            for ($i = 0; $i <= 31; $i++) {
                $body = str_replace(chr($i), '', $body);
            }
            $jsonStr = str_replace(chr(127), '', $body);

            // We've seen occurrences of this happen when the string is not UTF-8. Forcing it fixes it.
            // See https://github.com/Expensify/Expensify/issues/21805 for example.
            $json = json_decode(mb_convert_encoding($jsonStr, 'UTF-8', 'UTF-8'), true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new BedrockError('Could not parse JSON from bedrock');
            }
        }

        return $json;
    }

    private function extractResponseHeaders(array $responseHeaderLines)
    {
        $responseHeaders = [];
        foreach ($responseHeaderLines as $responseHeaderLine) {
            // Try to split this line
            $nameValue = explode(':', $responseHeaderLine, 2);
            if (count($nameValue) === 2) {
                $responseHeaders[trim($nameValue[0])] = trim($nameValue[1]);
            } elseif (strlen($responseHeaderLine)) {
                $this->logger->warning('Bedrock\Client - Malformed response header, ignoring.', ['responseHeaderLine' => $responseHeaderLine]);
            }
        }

        return $responseHeaders;
    }

    /**
     * Converts a string to UTF8.
     *
     * @param string $str
     *
     * @return string
     */
    private static function toUTF8($str)
    {
        // Get the current encoding, default to UTF-8 if we can't tell. Then convert
        // the string to UTF-8 and ignore any characters that can't be converted.
        $encoding = mb_detect_encoding($str) ?: 'UTF-8';

        return iconv($encoding, 'UTF-8//IGNORE', $str);
    }

    /**
     * When a host fails, we blacklist that server for a certain amount of time, so we don't send requests to it when we
     * know it's down. The blacklist time is a random amount of time between 1 second and the maxBlackListTimeout
     * configuration.
     * We also close and clear the socket from the cache, so we don't reuse it.
     *
     * @suppress PhanUndeclaredConstant - suppresses TRAVIS_RUNNING
     */
    private function markHostAsFailed(string $host)
    {
        $blacklistedUntil = time() + rand(1, $this->maxBlackListTimeout);
        if (!defined('TRAVIS_RUNNING') || !TRAVIS_RUNNING) {
            $apcuKey = self::APCU_CACHE_PREFIX.$this->clusterName;
            $hostConfigs = apcu_fetch($apcuKey);
            $hostConfigs[$host]['blacklistedUntil'] = $blacklistedUntil;
            apcu_store($apcuKey, $hostConfigs);
        }
        $this->logger->info('Bedrock\Client - Marking server as failed', ['host' => $host, 'time' => date('Y-m-d H:i:s', $blacklistedUntil)]);

        // Since there was a problem with the host and we want to talk to a different one, we close and clear the socket.
        if ($this->socket) {
            @socket_close($this->socket);
            $this->socket = null;
        }
    }

    /**
     * Returns the highest commitCount of each cluster name instantiated in this request.
     *
     * @return int[]
     */
    public static function getCommitCounts(): array
    {
        $commitCounts = [];
        foreach (self::$instances as $instance) {
            $commitCounts[$instance->getClusterName()][] = $instance->commitCount;
        }
        foreach ($commitCounts as $name => $values) {
            $commitCounts[$name] = max($values) ?? 0;
        }

        return array_filter($commitCounts);
    }
}
