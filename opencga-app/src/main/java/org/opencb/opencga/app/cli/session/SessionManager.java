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

package org.opencb.opencga.app.cli.session;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.config.client.HostConfig;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SessionManager {

    public static final String SESSION_FILENAME_SUFFIX = "_session.json";
    public static final String NO_TOKEN = "NO_TOKEN";
    private static final String NO_STUDY = "NO_STUDY";
    private static final String ANONYMOUS = "anonymous";
    private final ClientConfiguration clientConfiguration;
//    private OpenCGAClient openCGAClient;
    private final HostConfig hostConfig;
    private Path sessionFolder;
    private ObjectWriter objectWriter;
    private ObjectReader objectReader;
    private Logger logger;


    public SessionManager(ClientConfiguration clientConfiguration) throws ClientException {
        this(clientConfiguration, clientConfiguration.getCurrentHost());
    }

    public SessionManager(ClientConfiguration clientConfiguration, HostConfig hostConfig) {
        this.clientConfiguration = clientConfiguration;
        this.hostConfig = hostConfig;

        this.init();
    }

    private void init() {
        this.sessionFolder = Paths.get(System.getProperty("user.home"), ".opencga");
        logger = LoggerFactory.getLogger(SessionManager.class);

        // TODO should we validate host name provided?
        boolean validHost = false;
        if (clientConfiguration != null) {
            for (HostConfig hostConfig : clientConfiguration.getRest().getHosts()) {
                if (Objects.equals(hostConfig.getName(), this.hostConfig.getName())) {
                    validHost = true;
                    break;
                }
            }

            // TODO
//            Session session = this.getSession();
//            this.openCGAClient = new OpenCGAClient(clientConfiguration);
//            this.openCGAClient.setToken(session.getToken());
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

    private Session createEmptySession() {
        Session session = new Session();
        session.setHost(this.hostConfig.getUrl());
        session.setVersion(GitRepositoryState.getInstance().getBuildVersion());
        session.setTimestamp(System.currentTimeMillis());
        session.setStudies(new ArrayList());
        session.setCurrentStudy(NO_STUDY);
        session.setToken(NO_TOKEN);
        session.setUser(ANONYMOUS);

        return session;
    }

    public Path getSessionPath() {
        return getSessionPath(this.hostConfig.getName());
    }

    public Path getSessionPath(String hostName) {
        String sessionFileName = hostName + SESSION_FILENAME_SUFFIX;
        sessionFileName = sessionFileName.replaceAll("[^a-zA-Z0-9.-]+", "_"); // Sanitize host name
        return sessionFolder.resolve(sessionFileName);
    }

    public Session getSession() {
        return getSession(this.hostConfig);
    }

    public Session getSession(HostConfig hostConfig) {
        Path sessionPath = getSessionPath(hostConfig.getName());
        if (Files.exists(sessionPath)) {
            try {
                logger.debug("Retrieving session from file " + sessionPath);
                return objectReader.readValue(sessionPath.toFile());
            } catch (IOException e) {
                logger.debug("Could not parse the session file properly");
            }
        }
        logger.debug("Creating an empty session");
        Session session = createEmptySession();
        try {
            saveSession(session);
        } catch (IOException e) {
            logger.debug("Could not create the session file properly");
        }
        return session;
    }


    public void updateSessionToken(String token, HostConfig hostConfig) throws IOException {
        updateSessionToken(token, hostConfig, Collections.emptyMap());
    }

    public void updateSessionToken(String token, HostConfig hostConfig, Map<String, Object> attributes) throws IOException {
        // Get current Session and update token
        Session session = getSession(hostConfig);
        session.setToken(token);
        session.setAttributes(attributes);

        // Save updated Session
        saveSession(session);
    }

    public void saveSession(String user, String token, String refreshToken, List<String> studies, Map<String, Object> attributes)
            throws IOException {
        JwtPayload jwtPayload = new JwtPayload(token);
        String issuedAt = TimeUtils.getTime(jwtPayload.getIssuedAt(), "yyyy-MM-dd HH:mm:ss");
        String expiration = TimeUtils.getTime(jwtPayload.getExpirationTime(), "yyyy-MM-dd HH:mm:ss");

        Session session = new Session(this.hostConfig.getUrl(), user, token, refreshToken, issuedAt, expiration, studies, attributes);
        if (CollectionUtils.isNotEmpty(studies)) {
            session.setCurrentStudy(studies.get(0));
        } else {
            session.setCurrentStudy(NO_STUDY);
        }
        saveSession(session, this.hostConfig.getName());
    }

    public void refreshSession(String refreshToken, String hostname) throws IOException {
        Session session = getSession().setRefreshToken(refreshToken);
        saveSession(session, hostname);
    }

    public void saveSession() throws IOException {
        saveSession(getSession(), this.hostConfig.getName());
    }

    public void saveSession(Session session) throws IOException {
        saveSession(session, this.hostConfig.getName());
    }

    public RestResponse<AuthenticationResponse> saveSession(String user, AuthenticationResponse response, OpenCGAClient openCGAClient,
                                                            Map<String, Object> attributes) throws ClientException, IOException {
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        if (response != null) {
            List<String> studies = new ArrayList<>();
            logger.debug(response.toString());
            RestResponse<Project> projects = openCGAClient.getProjectClient().search(
                    new ObjectMap(ProjectDBAdaptor.QueryParams.OWNER.key(), user));

            if (projects.getResponses().get(0).getNumResults() == 0) {
                // We try to fetch shared projects and studies instead when the user does not own any project or study
                projects = openCGAClient.getProjectClient().search(new ObjectMap());
            }

            for (Project project : projects.getResponses().get(0).getResults()) {
                for (Study study : project.getStudies()) {
                    studies.add(study.getFqn());
                }
            }
            this.saveSession(user, response.getToken(), response.getRefreshToken(), studies, attributes);
            res.setType(QueryType.VOID);
        }
        return res;
    }

    public RestResponse<AuthenticationResponse> saveSession(String user, AuthenticationResponse response, OpenCGAClient openCGAClient)
            throws ClientException, IOException {
        return saveSession(user, response, openCGAClient, Collections.emptyMap());
    }

    public void saveSession(Session session, String hostName) throws IOException {
        // Check if ~/.opencga folder exists
        if (!Files.exists(sessionFolder)) {
            Files.createDirectory(sessionFolder);
        }
        Path sessionPath = getSessionPath(hostName);
        logger.debug("Saving '{}'", session);
        logger.debug("Session file '{}'", sessionPath.getFileName());

        objectWriter.writeValue(sessionPath.toFile(), session);
    }

    public void logoutSessionFile() throws IOException {
        // We just need to save an empty session, this will delete user and token for this host
        logger.debug("Session logout for host '{}'", hostConfig.getUrl());
        saveSession(createEmptySession(), hostConfig.getUrl());
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SessionManager{");
        sb.append("clientConfiguration=").append(clientConfiguration);
        sb.append(", hostConfig='").append(hostConfig).append('\'');
        sb.append(", Session='").append(getSession().toString()).append('\'');
        sb.append('}');
        return sb.toString();
    }


}
