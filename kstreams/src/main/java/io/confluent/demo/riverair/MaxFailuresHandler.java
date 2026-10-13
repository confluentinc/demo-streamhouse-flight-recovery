package io.confluent.demo.riverair;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.time.Duration;
import java.time.Instant;

/**
 * Replaces a failed stream thread, but shuts this instance down if its threads keep failing
 * (maxFailures within the interval) or the broker doesn't support the protocol. The other
 * instances keep serving and take over its tasks.
 */
final class MaxFailuresHandler implements StreamsUncaughtExceptionHandler {
    private final int maxFailures;
    private final Duration interval;
    private Instant firstFailure;
    private int failures;

    MaxFailuresHandler(int maxFailures, Duration interval) {
        this.maxFailures = maxFailures;
        this.interval = interval;
    }

    @Override
    public synchronized StreamThreadExceptionResponse handle(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null) cause = cause.getCause();
        if (cause instanceof UnsupportedVersionException) {
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        }
        Instant now = Instant.now();
        if (firstFailure == null || Duration.between(firstFailure, now).compareTo(interval) > 0) {
            firstFailure = now;
            failures = 0;
        }
        failures++;
        return failures >= maxFailures
                ? StreamThreadExceptionResponse.SHUTDOWN_CLIENT
                : StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
