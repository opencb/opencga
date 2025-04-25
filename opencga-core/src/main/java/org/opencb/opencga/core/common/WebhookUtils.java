package org.opencb.opencga.core.common;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class WebhookUtils {
    private static final Logger logger = LoggerFactory.getLogger(WebhookUtils.class);
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    /**
     * Send a message to a webhook.
     *
     * @param targetUrl The URL of the webhook
     * @param method    The HTTP method to use (GET, POST, etc.)
     * @param subject   The subject of the message
     * @param content   The content of the message
     * @return The HTTP response status code
     * @throws IOException If an I/O error occurs when sending the request
     */
    public static int send(String targetUrl, String method, String subject, String content) throws IOException {
        return send(targetUrl, method, subject, content, new HashMap<>());
    }

    /**
     * Send a message to a webhook with custom headers.
     *
     * @param targetUrl The URL of the webhook
     * @param method    The HTTP method to use (GET, POST, etc.)
     * @param subject   The subject of the message
     * @param content   The content of the message
     * @param headers   Custom headers to include in the request
     * @return The HTTP response status code
     * @throws IOException If an I/O error occurs when sending the request
     */
    public static int send(String targetUrl, String method, String subject, String content, Map<String, String> headers)
            throws IOException {
        if (StringUtils.isEmpty(targetUrl)) {
            throw new IllegalArgumentException("Target URL cannot be null or empty");
        }

        // Prepare request body in JSON format
        String jsonPayload = createJsonPayload(subject, content);

        // Create connection
        URL url = new URL(targetUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(DEFAULT_TIMEOUT_SECONDS * 1000);
        connection.setReadTimeout(DEFAULT_TIMEOUT_SECONDS * 1000);
        connection.setRequestMethod(method.toUpperCase());
        connection.setDoOutput(true);

        // Set headers
        connection.setRequestProperty("Content-Type", "application/json");
        for (Map.Entry<String, String> header : headers.entrySet()) {
            connection.setRequestProperty(header.getKey(), header.getValue());
        }

        // Set the method and body
        String uppercaseMethod = method.toUpperCase();
        if ("GET".equals(uppercaseMethod)) {
            connection.setDoOutput(false);
        } else if ("POST".equals(uppercaseMethod) || "PUT".equals(uppercaseMethod)) {
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
        } else if ("DELETE".equals(uppercaseMethod)) {
            // No body for DELETE
        } else {
            throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        }

        // Get response code
        int statusCode = connection.getResponseCode();

        // Read response for debugging
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        statusCode >= 400 ? connection.getErrorStream() : connection.getInputStream(),
                        StandardCharsets.UTF_8))) {
            String responseBody = br.lines().collect(Collectors.joining(System.lineSeparator()));
            logger.debug("Webhook response: {} - {}", statusCode, responseBody);
        } catch (IOException e) {
            logger.debug("Could not read response body", e);
        }

        return statusCode;
    }

    /**
     * Sends a webhook notification with automatic retry logic.
     *
     * @param targetUrl The URL of the webhook
     * @param method    The HTTP method to use
     * @param subject   The subject of the message
     * @param content   The content of the message
     * @param maxRetries Maximum number of retry attempts
     * @return true if the webhook was delivered successfully, false otherwise
     */
    public static boolean sendWithRetry(String targetUrl, String method, String subject, String content, int maxRetries) {
        int attempts = 0;
        boolean success = false;

        while (attempts <= maxRetries && !success) {
            try {
                int statusCode = send(targetUrl, method, subject, content);
                // Consider any 2xx status code as success
                if (statusCode >= 200 && statusCode < 300) {
                    success = true;
                } else {
                    logger.warn("Webhook request failed with status code: {}, attempt {}/{}", statusCode, attempts + 1, maxRetries + 1);
                }
            } catch (Exception e) {
                logger.error("Error sending webhook notification (attempt {}/{}): {}", attempts + 1, maxRetries + 1, e.getMessage());
            }

            if (!success && attempts < maxRetries) {
                // Exponential backoff: wait longer between each retry
                try {
                    Thread.sleep((long) Math.pow(2, attempts) * 1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            attempts++;
        }

        return success;
    }

    private static String createJsonPayload(String subject, String content) {
        return String.format("{\"subject\":\"%s\",\"text\":\"%s\"}",
                escapeJsonString(subject),
                escapeJsonString(content));
    }

    private static String escapeJsonString(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}