<?php

namespace Expensify\Bedrock\Jobs;

use UnexpectedValueException;

/**
 * Decodes the lossless job data snapshot returned by Bedrock.
 */
final class ExpectedDataSnapshot
{
    /**
     * Returns null for a legacy job that does not include a snapshot.
     *
     * @throws UnexpectedValueException
     */
    public static function fromJob(array $job): ?string
    {
        if (!array_key_exists('expectedDataBase64', $job)) {
            return null;
        }

        if (!is_string($job['expectedDataBase64'])) {
            throw new UnexpectedValueException('Bedrock returned a non-string expectedDataBase64 value');
        }

        $expectedData = base64_decode($job['expectedDataBase64'], true);
        if ($expectedData === false || $expectedData === '') {
            throw new UnexpectedValueException('Bedrock returned an invalid expectedDataBase64 value');
        }

        return $expectedData;
    }
}
