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
    /**
     * Sets the default configuration.
     *
     * @param array $config
     */
    public static function configure(array $config)
    {
        // Store the configuration
        self::$config = array_merge([
            'host' => 'localhost',
            'port' => 8888,
            'failoverHost' => 'localhost',
            'failoverPort' => 8888,
            'connectionTimeout' => 300,
            'readTimeout' => 300,
            'logger' => new NullLogger(),
            'stats' => new NullStats(),
            'requestID' => null,
            'writeConsistency' => 'ASYNC',
        ], self::$config, $config);
    }

    /**
     * @return LoggerInterface
     */
    public static function getLogger()
    {
        if (is_string(self::$config['logger'])) {
            self::$config['logger'] = new self::$config['logger'];
        }

        return self::$config['logger'];
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
        self::$config['logger'] = $logger;
    }

    /**
     * @return StatsInterface
     */
    public static function getStats()
    {
        if (is_string(self::$config['stats'])) {
            self::$config['stats'] = new self::$config['stats'];
        }

        return self::$config['stats'];
    }

    /**
     * Sets the global requestID, which is used if no instance requestID is given.
     */
    public static function setGlobalRequestID($globalRequestID)
    {
        // Override the global requestID
        self::$globalRequestID = $globalRequestID;
    }

    /**
     * Creates a reusable Bedrock instance.
     * All params are optional and values set in `configure` would be used if are not passed here.
     *
     * @param string          $host              First host we attempt to connect to
     * @param int             $port              First port we attempt to connect to
     * @param string          $failoverHost      Host we attempt if the primary fails
     * @param int             $failoverPort      Port we attempt if the primary fails
     * @param int             $connectionTimeout Timeout to use when connecting
     * @param int             $readTimeout       Timeout to use when reading
     * @param LoggerInterface $logger            Class to use for logging
     * @param StatsInterface  $stats             Class to use for statistics tracking
     * @param string          $requestID         RequestID to send to bedrock for consolidated logging
     * @param string          $writeConsistency  The bedrock write consistency we want to use
     *
     * @throws BedrockError
     */
    public function __construct($host = null, $port = null, $failoverHost = null, $failoverPort = null, $connectionTimeout = null, $readTimeout = null, LoggerInterface $logger = null, StatsInterface $stats = null, $requestID = null, $writeConsistency = null)
    {
        // Store these for future use
        $this->host             = $host ?: self::$config['host'];
        $this->port             = $port ?: self::$config['port'];
        $this->failoverHost     = $failoverHost ?: self::$config['failoverHost'];
        $this->failoverPort     = $failoverPort ?: self::$config['failoverPort'];
        $this->sendTimeout      = $connectionTimeout ?: self::$config['connectionTimeout'];
        $this->readTimeout      = $readTimeout ?: self::$config['readTimeout'];
        $this->logger           = $logger ?: self::getLogger();
        $this->stats            = $stats ?: self::getStats();
        $this->requestID        = $requestID ?: self::$config['requestID'];
        $this->writeConsistency = $writeConsistency ?: self::$config['writeConsistency'];

        // If failovers still not defined, just copy primary
        if (!$this->failoverHost) {
            $this->failoverHost = $this->host;
        }
        if (!$this->failoverPort) {
            $this->failoverPort = $this->port;
        }

        // Make sure one way or the other, everything is defined
        $this->getLogger()->debug("Bedrock::Bedrock( {$this->host}, {$this->port}, {$this->failoverHost}, {$this->failoverPort} )");
        if (!$this->host || !$this->port || !$this->failoverHost || !$this->failoverPort) {
            throw new BedrockError('Failed to construct Bedrock object');
        }
    }

    public function __destruct()
    {
        @socket_close($this->socket);
    }

    /**
     * Makes a direct call to Bedrock.
     *
     * @param string $method  Request method
     * @param array  $headers Request headers (optional)
     * @param string $body    Request body (optional)
     *
     * @return array JSON response, or null on error
     */
    public function call($method, $headers = [], $body = '')
    {
        // Start timing the entire end-to-end
        $timeStart = microtime(true);
        $this->getLogger()->info("Starting a bedrock request", ['command' => $method, 'headers' => $headers]);

        // Include the last CommitCount, if we have one
        if ($this->commitCount) {
            $headers['commitCount']  = $this->commitCount;
        }

        // Include the requestID for logging purposes
        if ($this->requestID) {
            $headers['requestID'] = $this->requestID;
        } else if (self::$globalRequestID) {
            $headers['requestID'] = self::$globalRequestID;
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

        // Try idempotent requests up to three times, everything else only once
        $numTries = @$headers['idempotent'] ? 3 : 1;
        $response = null;
        while($numTries-- && !$response) {
            // Catch any connection failures and retry, but ignore non-connection failures.
            try {
                // Do the request.  This is split up into separate functions so we can
                // profile them independently -- useful when diagnosing various network
                // conditions.
                $this->sendRawRequest($rawRequest);
                $response = $this->receiveRawResponse();
            }
            catch(ConnectionFailure $e) {
                // Failed to connect.  Are we retrying?
                if ($numTries) {
                    $this->getLogger()->warning("Failed to connect, send, or receive request; retrying $numTries more times", ['message' => $e->getMessage()]);
                } else {
                    $this->getLogger()->warning("Failed to connect, send, or receive request; not retrying", ['message' => $e->getMessage()]);
                    throw $e;
                }
            }
        }

        // Log how long this particular call took
        $processingTime = isset($response['headers']['processingTime']) ? $response['headers']['processingTime'] : 0;
        $serverTime     = isset($response['headers']['totalTime']) ? $response['headers']['totalTime'] : 0;
        $clientTime     = (int) (microtime(true) - $timeStart) * 1000;
        $networkTime    = $clientTime - $serverTime;
        $waitTime       = $serverTime - $processingTime;
        $this->getLogger()->info("Bedrock request finished", [
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
     * The PID that opened this socket.  This class is designed to be used in a
     * script that forks, so we need to avoid multiple processes using the same
     * socket by accident.
     *
     *  @var null|int
     */
    private $socketPID = null;

    /**
     * Hostname we attempt to connect to first.
     *
     *  @var null|string
     */
    private $host = null;

    /**
     * Port we attempt to connect to first.
     *
     *  @var null|int
     */
    private $port = null;

    /**
     * Hostname we attempt if the first didn't work.
     *
     *  @var null|string
     */
    private $failoverHost = null;

    /**
     * Port we attempt if the first didn't work.
     *
     *  @var null|int
     */
    private $failoverPort = null;

    /**
     * @var array
     */
    private static $config = [];

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var StatsInterface
     */
    private $stats;

    /**
     * @var null|string The requestID to send to bedrock. This can be used to get consolidated logging between the
     *                  PHP request and the bedrock request.
     */
    private $requestID;

    /**
     * @var null|string The requestID to send to bedrock. This can be used to get consolidated logging between the
     *                  PHP request and the bedrock request.  Used if requestID is not provided for this instance.
     */
    private static $globalRequestID;

    /**
     * @var string The bedrock write consistency we want to use.
     */
    private $writeConsistency;

    /**
     * Create and connect a socket (with failover).
     */
    private function reconnect()
    {
        // If we already have a socket and our PID hasn't changed (eg, we haven't forked), use that
        if ($this->socket && $this->socketPID == posix_getpid()) {
            $this->getLogger()->debug('Reusing existing socket');

            return;
        }
        $this->getLogger()->debug('Opening new socket');

        // Try to connect to the requested host
        $this->socketPID = posix_getpid();
        $this->socket = @socket_create(AF_INET, SOCK_STREAM, getprotobyname('tcp'));

        // Make sure we succeed to create a socket
        if ($this->socket === false) {
            $socketError = socket_strerror(socket_last_error());
            throw new ConnectionFailure("Could not connect to Bedrock primary or failover $socketError");
        }

        // Configure this socket and connect to the primary host, failing over if necessary
        socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => $this->sendTimeout, 'usec' => 0]);
        socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $this->readTimeout, 'usec' => 0]);
        if (!(@socket_connect($this->socket, $this->host, $this->port))) {
            // Connection to the primary host failed; try the failover
            $socketError = socket_strerror(socket_last_error($this->socket));
            $this->getLogger()->info("Failed to connect to '{$this->host}' ($socketError), trying failover...");
            if (!(@socket_connect($this->socket, $this->failoverHost, $this->failoverPort))) {
                // Connection to both the main host and the failover failed!
                $socketError = socket_strerror(socket_last_error($this->socket));
                throw new ConnectionFailure("Could not connect to Bedrock primary host ({$this->host}) or failover ({$this->failoverHost}): $socketError");
            }
        }
    }

    /**
     * Sends the request on the existing socket, if possible, else it reconnects.
     *
     * @param string $rawRequest
     *
     * @throws BedrockError
     */
    private function sendRawRequest($rawRequest)
    {
        // Try up to 3 times, reconnecting each time
        $attempts = 0;
        while (++$attempts < 3) {
            // Reconnect (if necessary) and send
            $this->reconnect();
            $bytesSent = socket_send($this->socket, $rawRequest, strlen($rawRequest), MSG_EOF);
            if ($bytesSent === false) {
                // Failed to send anything, let's try again (if we haven't exceeded our retry limit)
                $errorCode = socket_last_error();
                $errorMsg  = socket_strerror($errorCode);
                $this->getLogger()->warning('Bedrock socket_send error', [
                    'code' => $errorCode,
                    'msg' => $errorMsg,
                    'attempts' => $attempts,
                ]);

                // Free the socket on the system
                socket_close($this->socket);
                $this->socket = null;
            } else {
                // We sent something; can't retry else we might double-send the same request.  Let's make sure we sent the
                // whole thing, else there's a problem.
                if ($bytesSent == strlen($rawRequest)) {
                    // Cool, we sent the whole thing.
                    return;
                } else {
                    // Partial send -- abort
                    throw new BedrockError('Sent partial request to Bedrock, aborting.');
                }
            }
        }

        // Failed after three attempts
        throw new BedrockError("Failed to send request to Bedrock after $attempts, aborting.");
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
                $this->getLogger()->warning('Malformed response header, ignoring.', ['responseHeaderLine' => $responseHeaderLine]);
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
        $this->getLogger()->debug("Received '$response[codeLine]', waiting for $contentLength bytes");
        while (strlen($rawResponseBody) < $contentLength) {
            $rawResponseBody .= $this->recv();
        }
        if (strlen($rawResponseBody) != $contentLength) {
            $this->getLogger()->warning('Server sent more content than expected, ignoring.',
                ['Content-Length' => $contentLength, 'bodyLength' => strlen($rawResponseBody)]
            );
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
}
