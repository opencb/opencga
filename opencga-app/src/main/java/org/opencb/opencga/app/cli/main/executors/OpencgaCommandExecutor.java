/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.app.cli.main.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.CliSession;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.io.*;
import org.opencb.opencga.client.rest.OpenCGAClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 27/05/16.
 *
 * @author imedina
 */
public abstract class OpencgaCommandExecutor extends CommandExecutor {

    protected OpenCGAClient openCGAClient;

    protected AbstractOutputWriter writer;

    public OpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        this(options, false);
    }

    public OpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean skipDuration) {
        super(options, true);

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
                    this.writer = new JsonOutputWriter(writerConfiguration);
                    break;
                case "yaml":
                    this.writer = new YamlOutputWriter(writerConfiguration);
                    break;
                case "text":
                default:
                    this.writer = new TextOutputWriter(writerConfiguration);
                    break;
            }

//            CliSession cliSession = loadCliSessionFile();
            logger.debug("sessionFile = " + cliSession);
            if (StringUtils.isNotEmpty(options.sessionId)) {
                // Ignore session file. Overwrite with command line information (just sessionId)
                cliSession = new CliSession(null, options.sessionId);
                sessionId = options.sessionId;
                userId = null;

                openCGAClient = new OpenCGAClient(options.sessionId, clientConfiguration);
            } else if (cliSession != null) {
                // 'logout' field is only null or empty while no logout is executed
                if (StringUtils.isEmpty(cliSession.getLogout())) {
                    // no timeout checks
                    if (skipDuration) {
                        openCGAClient = new OpenCGAClient(cliSession.getSessionId(), clientConfiguration);
                        openCGAClient.setUserId(cliSession.getUserId());
                        if (options.sessionId == null) {
                            options.sessionId = cliSession.getSessionId();
                        }
                    } else {
                        int sessionDuration = clientConfiguration.getSessionDuration() * 1000;
                        long timestamp = cliSession.getTimestamp();
                        long now = System.currentTimeMillis();
                        if ((now - timestamp) >= sessionDuration) {
                            logger.warn("Session expired, too much time with not action");
                            openCGAClient = new OpenCGAClient(cliSession.getSessionId(), clientConfiguration);
                            openCGAClient.setUserId(cliSession.getUserId());
                            openCGAClient.logout();
                            logoutCliSessionFile();
                        } else {
                            logger.debug("Session ok!!");
//                            this.sessionId = cliSession.getSessionId();
                            openCGAClient = new OpenCGAClient(cliSession.getSessionId(), clientConfiguration);
                            openCGAClient.setUserId(cliSession.getUserId());

                            if (options.sessionId == null) {
                                options.sessionId = cliSession.getSessionId();
                            }
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
        }
    }


    protected String resolveStudy(String study) {
        if (StringUtils.isEmpty(study)) {
            if (StringUtils.isNotEmpty(clientConfiguration.getDefaultStudy())) {
                return clientConfiguration.getDefaultStudy();
            }
        } else {
            // study is not empty, let's check if it is an alias
            if (clientConfiguration.getAlias() != null && clientConfiguration.getAlias().size() > 0) {
                String[] studies = study.split(",");
                List<String> studyList = new ArrayList<>(studies.length);
                for (String s : studies) {
                    if (clientConfiguration.getAlias().containsKey(s)) {
                        studyList.add(clientConfiguration.getAlias().get(study));
                    } else {
                        studyList.add(s);
                    }
                }
                return StringUtils.join(studyList, ",");
            }
        }
        return study;
    }

    public void createOutput(QueryResponse queryResponse) {
        if (queryResponse != null) {
            writer.print(queryResponse);
        }
    }

}
