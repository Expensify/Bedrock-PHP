<?php

namespace Expensify\Bedrock;

/**
 * Encapsulates the built-in Status plugin for Bedrock.
 */
class Status extends Plugin
{
    /**
     * Generates a trivial response.
     *
     * @return array
     */
    public function ping()
    {
        return $this->client->call('Ping');
    }

    /**
     * Reads a lightweight backend-health snapshot from the Status command, used by the AIMD load
     * handler to pause fetching when the backend is saturated.
     *
     * @return array{state: string, commandCount: int, queueDepth: int}
     */
    public function getHealth()
    {
        return self::parseHealth($this->client->call('Status'));
    }

    /**
     * Extracts the health fields from a Client::call() response. Pure (no I/O) so it can be unit
     * tested. `queuedCommandList` may arrive already decoded (array) or as a JSON string depending
     * on how the body was parsed, so both are handled.
     *
     * @param array $response a Client::call() response (['code' => int, 'headers' => [], 'body' => mixed])
     *
     * @return array{state: string, commandCount: int, queueDepth: int}
     */
    public static function parseHealth(array $response)
    {
        $body = $response['body'] ?? [];
        if (!is_array($body)) {
            $body = json_decode((string) $body, true) ?: [];
        }

        $queued = $body['queuedCommandList'] ?? [];
        if (is_string($queued)) {
            $queued = json_decode($queued, true) ?: [];
        }

        return [
            'state' => (string) ($body['state'] ?? 'UNKNOWN'),
            'commandCount' => intval($body['commandCount'] ?? 0),
            'queueDepth' => is_array($queued) ? count($queued) : 0,
        ];
    }
}
