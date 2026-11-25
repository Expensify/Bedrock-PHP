<?php

namespace Expensify\Bedrock\Exceptions;

/**
 * Thrown when the circuit breaker for a cluster is open, so the client never sent the request.
 */
class CircuitBreakerOpen extends ConnectionFailure
{
}
