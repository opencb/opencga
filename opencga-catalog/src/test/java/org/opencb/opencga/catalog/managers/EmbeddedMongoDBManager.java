/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.managers;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.commands.MongodArguments;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.transitions.Start;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.util.Collections;

/**
 * Manages an embedded MongoDB instance for testing purposes.
 * This provides faster test execution and better isolation compared to external MongoDB.
 *
 * <p>Configuration via system properties:</p>
 * <ul>
 *   <li>opencga.test.embeddedMongo - Enable/disable embedded MongoDB (default: true)</li>
 *   <li>opencga.test.mongodb.version - MongoDB version to use (default: 7.0)</li>
 *   <li>opencga.test.mongo.log.level - MongoDB log level: error, warn, info, debug (default: error)</li>
 *   <li>opencga.test.mongo.verbose - Enable verbose logging for debugging (default: false)</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>
 * # Run tests with default (minimal) MongoDB logging
 * mvn test
 *
 * # Run tests with specific MongoDB version
 * mvn test -Dopencga.test.mongodb.version=5.0
 *
 * # Run tests with verbose MongoDB logging for debugging
 * mvn test -Dopencga.test.mongo.log.level=debug -Dopencga.test.mongo.verbose=true
 *
 * # Run tests with external MongoDB
 * mvn test -Dopencga.test.embeddedMongo=false
 * </pre>
 */
public class EmbeddedMongoDBManager {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedMongoDBManager.class);
    private static final String DEFAULT_MONGODB_VERSION = "7.0";

    // Store original streams so logger output can bypass the filter if needed
    private static final PrintStream ORIGINAL_OUT = System.out;
    private static final PrintStream ORIGINAL_ERR = System.err;

    private static EmbeddedMongoDBManager instance;
    private TransitionWalker.ReachedState<RunningMongodProcess> runningMongod;
    private int port;
    private final boolean enabled;
    private final String mongoVersion;
    private final boolean verbose;

    private EmbeddedMongoDBManager() {
        // Check if embedded MongoDB should be used (can be disabled via system property)
        this.enabled = Boolean.parseBoolean(System.getProperty("opencga.test.embeddedMongo", "true"));
        this.mongoVersion = System.getProperty("opencga.test.mongodb.version", DEFAULT_MONGODB_VERSION);
        this.verbose = Boolean.parseBoolean(System.getProperty("opencga.test.mongo.verbose", "false"));
    }

    public static synchronized EmbeddedMongoDBManager getInstance() {
        if (instance == null) {
            instance = new EmbeddedMongoDBManager();
        }
        return instance;
    }

    /**
     * Converts the configured version string to a Flapdoodle Version enum.
     */
    private Version.Main getMongoVersion() {
        switch (mongoVersion) {
            case "3.6":
                return Version.Main.V3_6;
            case "4.0":
                return Version.Main.V4_0;
            case "4.2":
                return Version.Main.V4_2;
            case "4.4":
                return Version.Main.V4_4;
            case "5.0":
                return Version.Main.V5_0;
            case "6.0":
                return Version.Main.V6_0;
            case "7.0":
                return Version.Main.V7_0;
            default:
                logger.warn("Unknown MongoDB version '{}', using default {}", mongoVersion, DEFAULT_MONGODB_VERSION);
                return Version.Main.V4_4;
        }
    }

    public synchronized void start() throws IOException {
        if (!enabled) {
            logger.info("Embedded MongoDB is disabled. Using external MongoDB instance.");
            return;
        }

        if (runningMongod != null) {
            logger.debug("Embedded MongoDB is already running on port {}", port);
            return;
        }

        try {
            // Output these logs BEFORE installing the filter so they're visible
            logger.info("Starting embedded MongoDB {} with replica set support", mongoVersion);

            // Find an available port before starting MongoDB
            port = findAvailablePort();
            logger.info("Found available port: {}", port);

            // NOW install the filtering streams (after initial logs are output)
            if (!verbose) {
                // Install filtering PrintStreams that suppress mongod JSON logs permanently
                System.setOut(new FilteringPrintStream(System.out));
                System.setErr(new FilteringPrintStream(System.err));
                logger.info("MongoDB output filtering enabled (use -Dopencga.test.mongo.verbose=true to see all logs)");
            }

            // Configure and start MongoDB with replica set enabled using the pre-allocated port
            runningMongod = Mongod.instance()
                    .withNet(Start.to(Net.class).initializedWith(Net.defaults().withPort(port)))
                    .withMongodArguments(Start.to(MongodArguments.class)
                            .initializedWith(MongodArguments.defaults()
                                    .withReplication(Storage.of("rs0", 10))))
                    .start(getMongoVersion());

            logger.info("Embedded MongoDB {} started on port {}, initializing replica set...", mongoVersion, port);

            // Wait a bit for MongoDB to be fully ready before initializing replica set
            Thread.sleep(500);

            // Initialize the replica set
            initializeReplicaSet();

            logger.info("Embedded MongoDB {} with replica set 'rs0' ready on port {}", mongoVersion, port);

            // Add shutdown hook to ensure cleanup
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (verbose) {
                    logger.info("Shutting down embedded MongoDB via shutdown hook");
                }
                stop();
            }));

        } catch (Exception e) {
            logger.error("Failed to start embedded MongoDB version {}", mongoVersion, e);
            throw new IOException("Failed to start embedded MongoDB", e);
        }
    }

    /**
     * Initializes the replica set configuration for the embedded MongoDB instance.
     * This is required for tests that use transactions or other replica set features.
     */
    private void initializeReplicaSet() {
        MongoClient mongoClient = null;
        try {
            // Connect without replica set parameter for initial setup
            String connectionString = String.format("mongodb://localhost:%d", port);
            mongoClient = MongoClients.create(connectionString);

            MongoDatabase adminDb = mongoClient.getDatabase("admin");

            // Configure replica set with a single member
            Document config = new Document("_id", "rs0")
                    .append("members", Collections.singletonList(
                            new Document("_id", 0)
                                    .append("host", "localhost:" + port)
                    ));

            // Initiate the replica set
            adminDb.runCommand(new Document("replSetInitiate", config));

            logger.debug("Replica set initiation command sent");

            // Wait for replica set to become PRIMARY (ready to accept operations)
            long timeout = System.currentTimeMillis() + 30000; // 30 seconds timeout
            while (System.currentTimeMillis() < timeout) {
                try {
                    Document result = adminDb.runCommand(new Document("replSetGetStatus", 1));
                    Integer myState = result.getInteger("myState");
                    if (myState != null && myState == 1) { // 1 = PRIMARY
                        logger.info("Replica set initialized successfully and is PRIMARY");
                        return;
                    }
                    logger.debug("Replica set state: {}, waiting for PRIMARY...", myState);
                } catch (Exception e) {
                    // Replica set might not be ready yet, continue waiting
                    logger.trace("Waiting for replica set to be ready: {}", e.getMessage());
                }
                Thread.sleep(100);
            }

            throw new RuntimeException("Replica set initialization timed out after 30 seconds");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Replica set initialization was interrupted", e);
        } catch (Exception e) {
            logger.error("Failed to initialize replica set", e);
            throw new RuntimeException("Could not initialize replica set", e);
        } finally {
            if (mongoClient != null) {
                try {
                    mongoClient.close();
                } catch (Exception e) {
                    logger.warn("Error closing MongoDB client", e);
                }
            }
        }
    }

    public synchronized void stop() {
        if (!enabled) {
            return;
        }

        if (runningMongod != null) {
            logger.info("Stopping embedded MongoDB on port {}", port);
            try {
                runningMongod.close();
            } catch (Exception e) {
                logger.warn("Error stopping embedded MongoDB", e);
            } finally {
                runningMongod = null;
            }
        }
    }

    public int getPort() {
        return port;
    }

    public String getConnectionString() {
        if (!enabled) {
            // Fall back to default MongoDB connection
            return "localhost:27017";
        }
        return "localhost:" + port;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isRunning() {
        return enabled && runningMongod != null;
    }

    /**
     * Finds an available port on the local machine.
     * This is used to work around issues with Flapdoodle's dynamic port allocation.
     *
     * @return an available port number
     * @throws IOException if unable to find an available port
     */
    private int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IOException("Failed to find an available port", e);
        }
    }

    /**
     * Custom PrintStream that filters out mongod JSON log lines while allowing other output through.
     * This prevents the noisy mongod logs from cluttering test output while preserving all other logs.
     */
    private static class FilteringPrintStream extends PrintStream {
        private final PrintStream original;
        private final StringBuilder lineBuffer = new StringBuilder();
        private boolean inLine = false;

        public FilteringPrintStream(PrintStream original) {
            super(original);
            this.original = original;
        }

        @Override
        public void write(int b) {
            if (b == '\n') {
                // End of line - check if we should filter it
                String line = lineBuffer.toString();
                boolean isMongodLog = line.contains("{\"t\":{\"$date\":") || line.startsWith("[mongod output]") || line.startsWith("[mongod error]");

                if (!isMongodLog) {
                    // Not a mongod log - write the buffered line and newline
                    original.print(lineBuffer.toString());
                    original.write(b);
                    original.flush();
                }
                // Clear buffer for next line
                lineBuffer.setLength(0);
                inLine = false;
            } else if (b == '\r') {
                // Carriage return - just pass through for now
                // Don't filter yet, wait for \n
            } else {
                // Regular character - add to buffer
                lineBuffer.append((char) b);
                inLine = true;
            }
        }

        @Override
        public void write(byte[] buf, int off, int len) {
            // Process byte by byte to maintain line buffering
            for (int i = 0; i < len; i++) {
                write(buf[off + i]);
            }
        }

        @Override
        public void flush() {
            // Flush any remaining content in buffer (partial line)
            if (lineBuffer.length() > 0) {
                String line = lineBuffer.toString();
                boolean isMongodLog = line.contains("{\"t\":{\"$date\":") || line.startsWith("[mongod output]") || line.startsWith("[mongod error]");

                if (!isMongodLog) {
                    original.print(lineBuffer.toString());
                }
                lineBuffer.setLength(0);
            }
            original.flush();
        }
    }
}
