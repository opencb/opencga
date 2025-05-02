package org.opencb.opencga.core.common;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class WebhookUtilsTest {

    private HttpServer server;
    private int port;
    private String baseUrl;

    @Before
    public void setUp() throws Exception {
        // Start a simple HTTP server for testing
        port = 8080;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        baseUrl = "http://localhost:" + port;
        server.start();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testSuccessfulPost() throws IOException {
        // Setup a context to handle the request
        CountDownLatch latch = new CountDownLatch(1);
        final String[] receivedSubject = {null};
        final String[] receivedContent = {null};

        server.createContext("/success", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if ("POST".equals(exchange.getRequestMethod())) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                        String body = reader.readLine();
                        // Simple check for subject and content
                        if (body.contains("\"subject\":\"Test Subject\"") && body.contains("\"text\":\"Test Content\"")) {
                            receivedSubject[0] = "Test Subject";
                            receivedContent[0] = "Test Content";
                        }
                    }

                    String response = "Success";
                    exchange.sendResponseHeaders(200, response.length());
                    exchange.getResponseBody().write(response.getBytes());
                    exchange.getResponseBody().close();
                }
                latch.countDown();
            }
        });

        // Call the webhook
        int statusCode = WebhookUtils.send(baseUrl + "/success", "POST", "Test Subject", "Test Content");

        // Wait for the server to process the request
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }

        // Verify the results
        assertEquals(200, statusCode);
        assertEquals("Test Subject", receivedSubject[0]);
        assertEquals("Test Content", receivedContent[0]);
    }

    @Test
    public void testCustomHeaders() throws IOException {
        // Setup a context to handle the request
        CountDownLatch latch = new CountDownLatch(1);
        final Map<String, String> receivedHeaders = new HashMap<>();

        server.createContext("/headers", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                receivedHeaders.put("Authorization", exchange.getRequestHeaders().getFirst("Authorization"));
                receivedHeaders.put("X-Custom-Header", exchange.getRequestHeaders().getFirst("X-Custom-Header"));

                String response = "Success";
                exchange.sendResponseHeaders(200, response.length());
                exchange.getResponseBody().write(response.getBytes());
                exchange.getResponseBody().close();
                latch.countDown();
            }
        });

        // Setup custom headers
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer test-token");
        headers.put("X-Custom-Header", "test-value");

        // Call the webhook with custom headers
        int statusCode = WebhookUtils.send(baseUrl + "/headers", "POST", "Test Subject", "Test Content", headers);

        // Wait for the server to process the request
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }

        // Verify the results
        assertEquals(200, statusCode);
        assertEquals("Bearer test-token", receivedHeaders.get("Authorization"));
        assertEquals("test-value", receivedHeaders.get("X-Custom-Header"));
    }

    @Test
    public void testDifferentHttpMethods() throws IOException {
        // Test GET
        server.createContext("/get", exchange -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                String response = "Success";
                exchange.sendResponseHeaders(200, response.length());
                exchange.getResponseBody().write(response.getBytes());
                exchange.getResponseBody().close();
            } else {
                exchange.sendResponseHeaders(405, 0);
                exchange.getResponseBody().close();
            }
        });

        // Test PUT
        server.createContext("/put", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                String response = "Success";
                exchange.sendResponseHeaders(200, response.length());
                exchange.getResponseBody().write(response.getBytes());
                exchange.getResponseBody().close();
            } else {
                exchange.sendResponseHeaders(405, 0);
                exchange.getResponseBody().close();
            }
        });

        // Test DELETE
        server.createContext("/delete", exchange -> {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                String response = "Success";
                exchange.sendResponseHeaders(200, response.length());
                exchange.getResponseBody().write(response.getBytes());
                exchange.getResponseBody().close();
            } else {
                exchange.sendResponseHeaders(405, 0);
                exchange.getResponseBody().close();
            }
        });

        // Test each method
        assertEquals(200, WebhookUtils.send(baseUrl + "/get", "GET", "Test Subject", "Test Content"));
        assertEquals(200, WebhookUtils.send(baseUrl + "/put", "PUT", "Test Subject", "Test Content"));
        assertEquals(200, WebhookUtils.send(baseUrl + "/delete", "DELETE", "Test Subject", "Test Content"));
    }

    @Test(expected = ProtocolException.class)
    public void testInvalidHttpMethod() throws IOException {
        WebhookUtils.send(baseUrl + "/invalid", "INVALID_METHOD", "Test Subject", "Test Content");
    }

    @Test
    public void testRetryMechanism() {
        // Setup a context that fails the first two times
        final int[] attemptCount = {0};
        server.createContext("/retry", exchange -> {
            attemptCount[0]++;
            try {
                if (attemptCount[0] <= 2) {
                    // First two attempts will fail
                    exchange.sendResponseHeaders(500, 0);
                } else {
                    // Third attempt will succeed
                    String response = "Success after retry";
                    exchange.sendResponseHeaders(200, response.length());
                    exchange.getResponseBody().write(response.getBytes());
                }
                exchange.getResponseBody().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Call with retry (max 3 attempts)
        boolean success = WebhookUtils.sendWithRetry(baseUrl + "/retry", "POST", "Test Subject", "Test Content", 3);

        // Verify results
        assertTrue(success);
        assertEquals(3, attemptCount[0]);
    }

    @Test
    public void testRetryFailure() {
        // Setup a context that always fails
        server.createContext("/retry-fail", exchange -> {
            try {
                exchange.sendResponseHeaders(500, 0);
                exchange.getResponseBody().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Call with retry (max 2 attempts)
        boolean success = WebhookUtils.sendWithRetry(baseUrl + "/retry-fail", "POST", "Test Subject", "Test Content", 2);

        // Verify results - should fail
        assertFalse(success);
    }

    @Test
    public void testEscapeJsonString() throws Exception {
        // Test escaping JSON special characters
        String input = "Test \"quoted\" and \n new line \t tab \\ backslash";
        String expected = "{\"subject\":\"Test \\\"quoted\\\" and \\n new line \\t tab \\\\ backslash\",\"text\":\"Test Content\"}";

        CountDownLatch latch = new CountDownLatch(1);
        final String[] receivedJson = {null};

        server.createContext("/escape", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                    receivedJson[0] = reader.readLine();
                }

                String response = "Success";
                exchange.sendResponseHeaders(200, response.length());
                exchange.getResponseBody().write(response.getBytes());
                exchange.getResponseBody().close();
                latch.countDown();
            }
        });

        WebhookUtils.send(baseUrl + "/escape", "POST", input, "Test Content");

        // Wait for the server to process the request
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }

        // Verify the escaping
        assertEquals(expected, receivedJson[0]);
    }

    @Test
    public void testNullHandling() throws IOException {
        CountDownLatch latch = new CountDownLatch(1);
        final String[] receivedJson = {null};

        server.createContext("/null", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                    receivedJson[0] = reader.readLine();
                }

                String response = "Success";
                exchange.sendResponseHeaders(200, response.length());
                exchange.getResponseBody().write(response.getBytes());
                exchange.getResponseBody().close();
                latch.countDown();
            }
        });

        // Call with null values
        WebhookUtils.send(baseUrl + "/null", "POST", null, null);

        // Wait for the server to process the request
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }

        // Verify null values are handled properly
        assertEquals("{\"subject\":\"\",\"text\":\"\"}", receivedJson[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyUrl() throws IOException {
        WebhookUtils.send("", "POST", "Test Subject", "Test Content");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullUrl() throws IOException {
        WebhookUtils.send(null, "POST", "Test Subject", "Test Content");
    }
}