package com.example.demo.eand.utill;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for handling retry logic with exponential backoff strategy.
 */
@Slf4j
public class RetryUtil {

    private static final long BASE_DELAY_MS = 1000; // 1 second
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final long MAX_DELAY_MS = 30000; // 30 seconds

    /**
     * Execute operation with retry logic and exponential backoff.
     *
     * @param operation    The operation to execute
     * @param maxRetries   Maximum number of retry attempts
     * @param operationName Name of the operation for logging
     * @return true if operation succeeded, false otherwise
     */
    public static boolean executeWithRetry(RetryableOperation operation, int maxRetries, String operationName) {

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                operation.execute();
                if (attempt > 0) {
                    log.info("RETRY: {} succeeded on attempt {}/{}", operationName, attempt + 1, maxRetries + 1);
                }
                return true; // Success
            } catch (Exception ex) {
                if (attempt == maxRetries) {
                    log.error("RETRY: {} failed after {} attempts. operationName={}",
                            operationName, maxRetries + 1, operationName, ex);
                    return false; // All retries exhausted
                }

                long delayMs = calculateBackoffDelay(attempt);
                log.warn("RETRY: {} failed (attempt {}/{}). Retrying after {}ms. Error: {}",
                        operationName, attempt + 1, maxRetries + 1, delayMs, ex.getMessage());

                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("RETRY: Interrupted during backoff delay", ie);
                    return false;
                }
            }
        }

        return false;
    }

    /**
     * Execute operation with retry logic and custom fixed delay.
     *
     * @param operation    The operation to execute
     * @param maxRetries   Maximum number of retry attempts
     * @param delayMs      Fixed delay between retries in milliseconds
     * @param operationName Name of the operation for logging
     * @return true if operation succeeded, false otherwise
     */
    public static boolean executeWithRetryFixedDelay(
            RetryableOperation operation,
            int maxRetries,
            long delayMs,
            String operationName) {

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                operation.execute();
                if (attempt > 0) {
                    log.info("RETRY: {} succeeded on attempt {}/{}", operationName, attempt + 1, maxRetries + 1);
                }
                return true; // Success
            } catch (Exception ex) {
                if (attempt == maxRetries) {
                    log.error("RETRY: {} failed after {} attempts. operationName={}",
                            operationName, maxRetries + 1, operationName, ex);
                    return false; // All retries exhausted
                }

                log.warn("RETRY: {} failed (attempt {}/{}). Retrying after {}ms. Error: {}",
                        operationName, attempt + 1, maxRetries + 1, delayMs, ex.getMessage());

                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("RETRY: Interrupted during delay", ie);
                    return false;
                }
            }
        }

        return false;
    }

    /**
     * Calculate exponential backoff delay with jitter.
     *
     * @param attemptNumber The current attempt number (0-based)
     * @return Delay in milliseconds
     */
    private static long calculateBackoffDelay(int attemptNumber) {
        long delay = (long) (BASE_DELAY_MS * Math.pow(BACKOFF_MULTIPLIER, attemptNumber));
        delay = Math.min(delay, MAX_DELAY_MS); // Cap max delay
        
        // Add jitter (±10%)
        long jitter = (long) (delay * 0.1 * (Math.random() - 0.5) * 2);
        return Math.max(BASE_DELAY_MS, delay + jitter);
    }

    /**
     * Functional interface for retryable operations.
     */
    @FunctionalInterface
    public interface RetryableOperation {
        void execute() throws Exception;
    }
}
