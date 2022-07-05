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

package org.opencb.opencga.app.cli.main.impl;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.app.cli.main.config.IClientConfiguration;
import org.opencb.commons.app.cli.main.utils.CommandLineUtils;
import org.opencb.commons.app.cli.session.AbstractSessionManager;
import org.opencb.commons.app.cli.session.Session;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.config.HostConfig;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;

import static org.opencb.commons.utils.PrintUtils.*;

public class SessionManagerImpl extends AbstractSessionManager {


    public SessionManagerImpl(IClientConfiguration clientConfiguration, String host) {
        super(clientConfiguration, host);
    }

    public static String getHost() {
        return "";
    }

    public void init() {
        this.sessionFolder = Paths.get(System.getProperty("user.home"), ".opencga");
        logger = LoggerFactory.getLogger(SessionManagerImpl.class);

        // TODO should we validate host name provided?
        boolean validHost = false;
        if (clientConfiguration != null) {
            for (HostConfig hostConfig : ((ClientConfiguration) clientConfiguration).getRest().getHosts()) {
                if (Objects.equals(hostConfig.getName(), host)) {
                    validHost = true;
                    break;
                }
            }
        } else {
            CommandLineUtils.error("The client configuration can not be null. Please check configuration file.");
            System.exit(-1);
        }


        // Prepare objects for writing and reading sessions
        if (validHost) {
            ObjectMapper objectMapper = new ObjectMapper();
            this.objectWriter = objectMapper
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .writerFor(Session.class)
                    .withDefaultPrettyPrinter();
            this.objectReader = objectMapper
                    .readerFor(Session.class);
        } else {
            CommandLineUtils.error("Not valid host. Please check configuration file or host parameter.", null);
            System.exit(-1);
        }
    }

    public void setValidatedCurrentStudy(String arg) {
        Session session = getSession();
        if (!StringUtils.isEmpty(session.getToken())) {
            logger.debug("Check study " + arg);
            OpenCGAClient openCGAClient = new OpenCGAClient(new AuthenticationResponse(session.getToken()), ((ClientConfiguration) clientConfiguration));
            if (openCGAClient != null) {
                try {
                    RestResponse<Study> res = openCGAClient.getStudyClient().info(arg, new ObjectMap());
                    if (res.allResultsSize() > 0) {
                        session.setCurrentStudy(res.response(0).getResults().get(0).getFqn());
                        logger.debug("Info study results: " + res.response(0).getResults().get(0).getFqn());
                        logger.debug("Validated study " + arg);
                        saveSession(session);

                        logger.debug("Current study is: " +
                                session.getCurrentStudy());
                        println(getKeyValueAsFormattedString("Current study is: ",
                                session.getCurrentStudy()));
                    } else {
                        printWarn("Invalid study");
                    }
                } catch (ClientException e) {
                    CommandLineUtils.error(e);
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    CommandLineUtils.error(e);
                    logger.error(e.getMessage(), e);
                }
            } else {
                printError("Client not available");
                logger.error("Client not available");
            }
        } else {
            printWarn("To set a study you must be logged in");
        }
    }
}
