/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.JCommander;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.io.*;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.rest.OpenCGAClient;

import java.io.IOException;

/**
 * Created on 27/05/16.
 *
 * @author imedina
 */
public abstract class OpencgaCommandExecutor extends CommandExecutor {

    protected OpenCGAClient openCGAClient;
    protected CatalogManager catalogManager;
//    protected ClientConfiguration clientConfiguration;
    protected AbstractWriter writer;

    public OpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        this(options, false);
    }

    public OpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean skipDuration) {
        super(options);
        init(options, skipDuration);
    }

    private void init(GeneralCliOptions.CommonCommandOptions options, boolean skipDuration) {
        try {

            WriterConfiguration writerConfiguration = new WriterConfiguration();
            writerConfiguration.setMetadata(options.metadata);
            writerConfiguration.setHeader(!options.noHeader);

            switch (options.outputFormat.toLowerCase()) {
                case "json_pretty":
                    writerConfiguration.setPretty(true);
                case "json":
                    this.writer = new JsonWriter(writerConfiguration);
                    break;
                case "yaml":
                    this.writer = new YamlWriter(writerConfiguration);
                    break;
                case "text":
                default:
                    this.writer = new TextWriter(writerConfiguration);
                    break;
            }

//            loadClientConfiguration();
            loadCatalogConfiguration();

            SessionFile sessionFile = loadSessionFile();
            logger.debug("sessionFile = " + sessionFile);
            if (sessionFile != null) {
                if (sessionFile.getLogout() == null) {
                    if (skipDuration) {
                        openCGAClient = new OpenCGAClient(sessionFile.getSessionId(), clientConfiguration);
                        openCGAClient.setUserId(sessionFile.getUserId());
                        if (options.sessionId == null) {
                            options.sessionId = sessionFile.getSessionId();
                        }
                    } else {
                        int sessionDuration = clientConfiguration.getSessionDuration() * 1000;
                        long timestamp = sessionFile.getTimestamp();
                        long now = System.currentTimeMillis();
                        if ((now - timestamp) >= sessionDuration) {
                            logger.warn("Session expired, too much time with not action");
                            openCGAClient = new OpenCGAClient(sessionFile.getSessionId(), clientConfiguration);
                            openCGAClient.setUserId(sessionFile.getUserId());
                            openCGAClient.logout();
                            logoutSessionFile();
//                        logoutSession();
                        } else {
                            logger.debug("Session ok!!");
                            this.sessionId = sessionFile.getSessionId();
                            openCGAClient = new OpenCGAClient(sessionFile.getSessionId(), clientConfiguration);
                            openCGAClient.setUserId(sessionFile.getUserId());

                            if (options.sessionId == null) {
                                options.sessionId = sessionFile.getSessionId();
                            }
                            // Some operations such as copy and link are run in the server side and need Catalog Manager
                            catalogManager = new CatalogManager(catalogConfiguration);
                        }
                    }
                } else {
                    logger.debug("Session already closed");
                    openCGAClient = new OpenCGAClient(clientConfiguration);
                }
            } else {
                logger.debug("No Session file");
                openCGAClient = new OpenCGAClient(clientConfiguration);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CatalogException e) {
            e.printStackTrace();
        }
    }

//    /**
//     * This method attempts to first data configuration from CLI parameter, if not present then uses
//     * the configuration from installation directory, if not exists then loads JAR storage-configuration.yml.
//     *
//     * @throws IOException If any IO problem occurs
//     */
//    public void loadClientConfiguration() throws IOException {
//        // We load configuration file either from app home folder or from the JAR
//        Path path = Paths.get(this.conf).resolve("client-configuration.yml");
//        if (path != null && Files.exists(path)) {
//            logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
//            this.clientConfiguration = ClientConfiguration.load(new FileInputStream(path.toFile()));
//        } else {
//            logger.debug("Loading configuration from JAR file");
//            this.clientConfiguration = ClientConfiguration
//                    .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("client-configuration.yml"));
//        }
//    }

    public void createOutput(QueryResponse queryResponse) {
        if (queryResponse != null) {
            writer.print(queryResponse);
        }
    }

    @Deprecated
    protected void checkSessionValid() throws Exception {
        SessionFile sessionFile = loadSessionFile();
        if (sessionFile == null || sessionFile.getLogout() != null) {
            System.out.println("No logged, please login first");
        } else {
            int sessionDuration = clientConfiguration.getSessionDuration();
            long timestamp = sessionFile.getTimestamp();
            long now = System.currentTimeMillis();
            if ((now - timestamp) >= sessionDuration * 1000) {
                System.out.println("Too much time with not action");
                logoutSession();
                throw new Exception("Logged out");
            }
        }
    }

    @Deprecated
    protected void logoutSession() throws IOException {
        SessionFile sessionFile = loadSessionFile();
        openCGAClient.logout();

        super.logoutSessionFile();
    }

    public static String getParsedSubCommand(JCommander jCommander) {
        String parsedCommand = jCommander.getParsedCommand();
        if (jCommander.getCommands().containsKey(parsedCommand)) {
            String subCommand = jCommander.getCommands().get(parsedCommand).getParsedCommand();
            return subCommand != null ? subCommand: "";
        } else {
            return "";
        }
    }
}
