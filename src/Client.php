<?php

namespace Expensify\Bedrock;

use Expensify\Bedrock\Exceptions\BedrockError;
use Expensify\Bedrock\Exceptions\ConnectionFailure;
use Expensify\Bedrock\Stats\NullStats;
use Expensify\Bedrock\Stats\StatsInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Client for communicating with bedrock.
 */
class Client implements LoggerAwareInterface
{
    private static $cachedHosts = [];
    private static $defaultConfig = [];

    /**
     * Name of the bedrock cluster we are talking to. If you have more than one bedrock cluster, you can pass in different
     * names for them in order to have separate statistics collected and caches of failed servers.
     * @var null|string
     */
    private $clusterName = null;

    /**
     * What is the last commit count of the node we talked to.
     *
     * This is used to ensure if we make a subsequent request to a different
     * node in the same session, that the node waits until it is at least as
     * "fresh" as the node we originally queried.
     *
     *  @var null|string
     */
    private $commitCount = null;

    /**
     * Existing socket, if any.  It's reused each time, if possible.
     *
     *  @var null|resource
     */
    private $socket = null;

    /**
     * List of hosts to use as first choice.
     *
     *  @var array
     */
    private $hosts = [];

    /**
     * List of failovers we attempt if the first didn't work.
     *
     *  @var array
     */
    private $failovers = [];

    private $connectionTimeout;

    private $readTimeout;

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

    private $lastHostUsed;

    /**
     * Creates a reusable Bedrock instance.
     * All params are optional and values set in `configure` would be used if are not passed here.
     *
     * @param array $config Configuration to use, can have all of these:
     * string               clusterName         Name of the bedrock cluster. This is used to separate requests made to
     *                                          different bedrock clusters.
     * array|null           hosts               List of hosts to attempt first
     * array|null           failovers           List of hosts to use as failovers
     * int|null             connectionTimeout   Timeout to use when connecting
     * int|null             readTimeout         Timeout to use when reading
     * LoggerInterface|null logger              Class to use for logging
     * StatsInterface|null  stats               Class to use for statistics tracking
     * string|null          writeConsistency    The bedrock write consistency we want to use
     * int|null             maxBlackListTimeout When a host fails, it will blacklist it and not try to reuse it for up
     *                                          to this amount of seconds.
     *
     * @throws BedrockError
     */
    public function __construct(array $config = [])
    {
        $config = array_merge(self::$defaultConfig, $config);
        $this->clusterName = $config['clusterName'];
        $this->hosts = $config['hosts'];
        $this->failovers = $config['failovers'];
        $this->connectionTimeout = $config['connectionTimeout'];
        $this->readTimeout = $config['readTimeout'];
        $this->logger = $config['logger'];
        $this->stats = $config['stats'];
        $this->writeConsistency = $config['writeConsistency'];
        $this->maxBlackListTimeout = $config['maxBlackListTimeout'];

        // Make sure we have at least one host configured
        $this->logger->debug("Bedrock::Bedrock $this->clusterName", ['hosts' => $this->hosts, 'failovers' => $this->failovers]);
        if (empty($this->hosts)) {
            throw new BedrockError('Failed to construct Bedrock object');
        }
    }

    public function __destruct()
    {
        @socket_close($this->socket);
    }

    /**
     * Sets the default config to use, these are used as defaults each time you create a new instance.
     */
    public static function configure(array $config)
    {
        // Store the configuration
        self::$defaultConfig = array_merge([
            'clusterName' => 'bedrock',
            'hosts' => ['localhost' => ['timeout' => 0, 'port' => 8888]],
            'failovers' => ['localhost' => ['timeout' => 0, 'port' => 8888]],
            'connectionTimeout' => 1,
            'readTimeout' => 300,
            'logger' => new NullLogger(),
            'stats' => new NullStats(),
            'writeConsistency' => 'ASYNC',
            'maxBlackListTimeout' => 1,
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
     *
     * @param LoggerInterface $logger
     *
     * @return null
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * @return StatsInterface
     */
    public function getStats()
    {
        if (is_string($this->stats)) {
            $this->stats = new $this->stats;
        }

        return $this->stats;
    }

    /**
     * Makes a direct call to Bedrock.
     *
     * @param string $method  Request method
     * @param array  $headers Request headers (optional)
     * @param string $body    Request body (optional)
     *
     * @return array JSON response, or null on error
     * @throws BedrockError
     */
    public function call($method, $headers = [], $body = '')
    {
        // Start timing the entire end-to-end
        $timeStart = microtime(true);
        $this->logger->info("Starting a bedrock request", ['command' => $method, 'headers' => $headers]);

        // Include the last CommitCount, if we have one
        if ($this->commitCount) {
            $headers['commitCount']  = $this->commitCount;
        }

        // Include the requestID for logging purposes
        if (isset($GLOBALS['REQUEST_ID'])) {
            $headers['requestID'] = $GLOBALS['REQUEST_ID'];
        }

        // Set the write consistency
        if ($this->writeConsistency) {
            $headers['writeConsistency'] = $this->writeConsistency;
        }

        // Construct the request
        $rawRequest = "$method\r\n";
        foreach ($headers as $name => $value) {
            if (is_array($value)) {
                $rawRequest .= "$name: ".addcslashes(json_encode($value), "\\")."\r\n";
            } elseif (is_bool($value)) {
                $rawRequest .= "$name: ".($value ? 'true' : 'false')."\r\n";
            } elseif ($value === null || $value === '') {
                // skip empty values
            } else {
                $rawRequest .= "$name: ".self::toUTF8(addcslashes($value, "\r\n\t\\"))."\r\n";
            }
        }
        $rawRequest .= "Content-Length: ".strlen($body)."\r\n";
        $rawRequest .= "\r\n";
        $rawRequest .= $body;

        $numTries = 3;
        $response = null;
        $lastTryException = null;
        while($numTries-- && !$response) {
            try {
                // Do the request.  This is split up into separate functions so we can
                // profile them independently -- useful when diagnosing various network
                // conditions.
                $this->sendRawRequest($rawRequest);
                $response = $this->receiveRawResponse();

                // Record the last error in the response as this affects how we
                // handle errors on this command
                if ($lastTryException) {
                    $response['lastTryException'] = $lastTryException;
                }
            } catch(ConnectionFailure $e) {
                // The error happened during connection (or before we sent any data) so we can retry it safely
                $this->markHostAsFailed();
                if ($numTries) {
                    $this->logger->warning("Failed to connect or send the request; retrying", ['message' => $e->getMessage(), 'retriesLeft' => $numTries]);
                    $lastTryException = $e;
                } else {
                    $this->logger->error("Failed to connect or send the request; not retrying", ['message' => $e->getMessage()]);
                    throw $e;
                }
            } catch(BedrockError $e) {
                // This error happen after sending some data to the server, so we only can retry it if it is an idempotent command
                $this->markHostAsFailed();
                if ($numTries && ($headers['idempotent'] ?? false)) {
                    $this->logger->warning("Failed to send the whole request or to receive it; retrying", ['message' => $e->getMessage(), 'retriesLeft' => $numTries]);
                    $lastTryException = $e;
                } else {
                    $this->logger->error("Failed to send the whole request or to receive it; not retrying", ['message' => $e->getMessage()]);
                    throw $e;
                }
            }
        }

        // Log how long this particular call took
        $processingTime = isset($response['headers']['processTime']) ? $response['headers']['processTime'] : 0;
        $serverTime     = isset($response['headers']['totalTime']) ? $response['headers']['totalTime'] : 0;
        $clientTime     = (int) (microtime(true) - $timeStart) * 1000;
        $networkTime    = $clientTime - $serverTime;
        $waitTime       = $serverTime - $processingTime;
        $this->logger->info("Bedrock request finished", [
            'command' => $method,
            'jsonCode' => isset($response['codeLine']) ? $response['codeLine'] : null,
            'duration' => $clientTime,
            'net' => $networkTime,
            'wait' => $waitTime,
            'proc' => $processingTime,
        ]);

        // Done!
        return $response;
    }

    /**
     * Create and connect a socket (with failovers).
     *
     * @throws ConnectionFailure
     */
    private function reconnect()
    {
        $this->logger->debug('Opening new socket');

        $hosts = $this->getPossibleHosts();
        $socketErrorCode = 1;
        $host = null;
        while ($socketErrorCode !== 0) {
            // Try to connect to the requested host
            if ($this->socket) {
                socket_close($this->socket);
                $this->socket = null;
            }
            $this->socket = @socket_create(AF_INET, SOCK_STREAM, getprotobyname('tcp'));

            // Make sure we succeed to create a socket
            if ($this->socket === false) {
                $socketError = socket_strerror(socket_last_error());
                throw new ConnectionFailure("Could not connect to create socket: $socketError");
            }

            // Configure this socket and try all the possible hosts in order, till we find one that works or all of them fail
            socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => $this->connectionTimeout, 'usec' => 0]);
            socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $this->readTimeout, 'usec' => 0]);

            reset($hosts);
            $host = key($hosts);
            @socket_connect($this->socket, $host, $hosts[$host]['port']);
            $socketErrorCode = socket_last_error($this->socket);
            socket_clear_error($this->socket);
            $this->lastHostUsed = $host;
            if ($socketErrorCode) {
                $socketError = socket_strerror($socketErrorCode);
                $this->logger->info("Failed to connect to host", ['host' => $host, 'port' => $hosts[$host]['port'], 'error' => $socketError]);
                $this->markHostAsFailed();
                array_shift($hosts);
            }

            // Nothing we can do, we couldn't connect to any host
            if (empty($hosts)) {
                throw new ConnectionFailure("Could not connect to Bedrock hosts or failovers");
            }
        }
    }

    /**
     * Sends the request on the existing socket, if possible, else it reconnects.
     *
     * @param string $rawRequest
     *
     * @throws ConnectionFailure
     * @throws BedrockError
     */
    private function sendRawRequest($rawRequest)
    {
        $this->reconnect();
        $bytesSent = socket_send($this->socket, $rawRequest, strlen($rawRequest), MSG_EOF);

        // Failed to send anything
        if ($bytesSent === false) {
            $errorCode = socket_last_error();
            $errorMsg  = socket_strerror($errorCode);
            $this->logger->warning('Bedrock socket_send error', [
                'code' => $errorCode,
                'msg' => $errorMsg,
            ]);
            throw new ConnectionFailure('Failed to send request to Bedrock');
        }

        // We sent something; can't retry or else we might double-send the same request. Let's make sure we sent the
        // whole thing, else there's a problem.
        if ($bytesSent != strlen($rawRequest)) {
            throw new BedrockError('Sent partial request to Bedrock');
        }
    }

    private function getPossibleHosts()
    {
        // If cached hosts aren't set, get them from the APC cache or init them from the config if they are not set or outdated
        if ((!defined('TRAVIS_RUNNING') || !TRAVIS_RUNNING) && empty(self::$cachedHosts[$this->clusterName])) {
            $apcuKey = 'bedrockFailoverHosts-'.$this->clusterName;
            self::$cachedHosts[$this->clusterName] = apcu_fetch($apcuKey) ?: [];
            $this->logger->info('APC fetch failover hosts', self::$cachedHosts[$this->clusterName]);

            // If the hosts and ports in the cache don't match the ones in the config, reset the cache.
            $savedHostsAndPort = [];
            foreach (self::$cachedHosts[$this->clusterName] as $host => $config) {
                $savedHostsAndPort[$host] = $config['port'];
            }
            asort($savedHostsAndPort);
            $configHostsAndPort = [];
            foreach (array_merge($this->hosts, $this->failovers) as $host => $config) {
                $configHostsAndPort[$host] = $config['port'];
            }
            asort($configHostsAndPort);
            if ($savedHostsAndPort !== $configHostsAndPort) {
                self::$cachedHosts[$this->clusterName] = array_merge($this->hosts, $this->failovers);
                $this->logger->info('APC init failover hosts', self::$cachedHosts[$this->clusterName]);
                apcu_store($apcuKey, self::$cachedHosts[$this->clusterName]);
            }
        }

        // Get one main host and all the failovers, then remove any of them that we know already failed.
        // Assemble the list of servers we'll try, in order.  First, pick one of the main hosts. We pick randomly
        // because we want to equally balance each server across all of its local databases. This allows us to have an
        // unequal number of servers and databases in a given datacenter. Also, we only pick one (versus trying both)
        // because if our first attempt fails we want to equally balance across *all* databases -- including the remote
        // ones. Otherwise if a database node goes down, the other databases in the same datacenter would get more load
        // (whereas this approach ensures the load is spread evenly across all).
        $failovers = array_keys($this->failovers);
        shuffle($failovers);
        $mainHost = array_rand($this->hosts);
        $hosts = array_merge([$mainHost], $failovers);

        $nonBlackListedHosts = [];
        foreach ($hosts as $host) {
            $timeout = self::$cachedHosts[$this->clusterName][$host]['timeout'] ?? null;
            if (!$timeout || $timeout < time()) {
                $nonBlackListedHosts[$host] = self::$cachedHosts[$this->clusterName][$host];
            }
        }

        return $nonBlackListedHosts;
    }

    /**
     * Receives a little bit more data.
     *
     * @return string The new data received.
     *
     * @throws ConnectionFailure
     */
    private function recv()
    {
        // Get more data from the socket
        $buf = null;
        $numRecv = @socket_recv($this->socket, $buf, 1024 * 1024, 0); // Read up to 1MB per call
        if ($numRecv === false) {
            $errorCode = socket_last_error();
            $errorMsg = socket_strerror($errorCode);
            throw new ConnectionFailure("Socket read failed: '$errorMsg' #$errorCode");
        } elseif ($numRecv <= 0) {
            throw new ConnectionFailure("Socket read failed: no data returned");
        }

        return $buf;
    }

    /**
     * Receives and parses the response.
     *
     * @return array Response object including 'code', 'codeLine', 'headers', and 'body'
     */
    private function receiveRawResponse()
    {
        // We'll populate this object with the results
        $response = [];

        // First, receive the headers -- until we get \r\n\r\n
        $rawResponse = '';
        do {
            // Get a little more
            $rawResponse .= $this->recv();
            $headerEnd = strpos($rawResponse, "\r\n\r\n");
        } while ($headerEnd === false);

        // Separate the headers from the body
        $rawResponseHeaders = substr($rawResponse, 0, $headerEnd);
        $rawResponseBody    = substr($rawResponse, $headerEnd + 4);

        // Parse the headers.  Take the first line as the code, the rest as name/value pairs.  In the
        $responseHeaderLines = explode("\r\n", $rawResponseHeaders);
        $response['codeLine'] = array_shift($responseHeaderLines);
        $response['code']     = intval($response['codeLine']);
        $response['headers']  = [];
        foreach ($responseHeaderLines as $responseHeaderLine) {
            $nameValue = explode(':', $responseHeaderLine);
            if (count($nameValue) != 2) {
                $this->logger->warning('Malformed response header, ignoring.', ['responseHeaderLine' => $responseHeaderLine]);
            } else {
                $response['headers'][ trim($nameValue[0]) ] = trim($nameValue[1]);
            }
        }

        // Capture the latest commit count
        if (isset($response['headers']['commitCount'])) {
            $this->commitCount = $response['headers']['commitCount'];
        }

        // Get the content length, and then keep receiving more body until we get the full length
        $contentLength = isset($response['headers']['Content-Length']) ? $response['headers']['Content-Length'] : 0;
        $this->logger->debug("Received '$response[codeLine]', waiting for $contentLength bytes");
        while (strlen($rawResponseBody) < $contentLength) {
            $rawResponseBody .= $this->recv();
        }
        if (strlen($rawResponseBody) != $contentLength) {
            $this->logger->warning('Server sent more content than expected, ignoring.', [
                'Content-Length' => $contentLength,
                'bodyLength' => strlen($rawResponseBody),
                'response' => $rawResponseBody,
            ]);
        }

        // If there is a body, let's parse it
        $response['rawBody'] = $rawResponseBody;
        $response['body']    = strlen($rawResponseBody) ? json_decode($rawResponseBody, true) : [];

        // Done!
        return $response;
    }

    /**
     * Converts a string to UTF8.
     *
     * @param string $str
     * @return string
     */
    private static function toUTF8($str)
    {
        // Get the current encoding, default to UTF-8 if we can't tell. Then convert
        // the string to UTF-8 and ignore any characters that can't be converted.
        $encoding = mb_detect_encoding($str) ?: 'UTF-8';

        return iconv($encoding, 'UTF-8//IGNORE', $str);
    }

    private function markHostAsFailed()
    {
        self::$cachedHosts[$this->clusterName][$this->lastHostUsed]['timeout'] = time() + rand(1, $this->maxBlackListTimeout);
        apcu_store('bedrockFailoverHosts-'.$this->clusterName, self::$cachedHosts[$this->clusterName]);
    }
}
