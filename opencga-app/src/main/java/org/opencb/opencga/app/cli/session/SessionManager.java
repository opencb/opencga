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
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.config.HostConfig;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.GitRepositoryState;
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
    private final String host;
    private Path sessionFolder;
    private ObjectWriter objectWriter;
    private ObjectReader objectReader;
    private Logger logger;


    public SessionManager(ClientConfiguration clientConfiguration) throws ClientException {
        this(clientConfiguration, clientConfiguration.getCurrentHost().getName());
    }

    public SessionManager(ClientConfiguration clientConfiguration, String host) {
        this.clientConfiguration = clientConfiguration;
        this.host = host;

        this.init();
    }

    private void init() {
        this.sessionFolder = Paths.get(System.getProperty("user.home"), ".opencga");
        logger = LoggerFactory.getLogger(SessionManager.class);

        // TODO should we validate host name provided?
        boolean validHost = false;
        if (clientConfiguration != null) {
            for (HostConfig hostConfig : clientConfiguration.getRest().getHosts()) {
                if (Objects.equals(hostConfig.getName(), host)) {
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
        session.setHost(host);
        session.setVersion(GitRepositoryState.get().getBuildVersion());
        session.setTimestamp(System.currentTimeMillis());
        session.setStudies(new ArrayList());
        session.setCurrentStudy(NO_STUDY);
        session.setToken(NO_TOKEN);
        session.setUser(ANONYMOUS);

        return session;
    }

    public Path getSessionPath() {
        return getSessionPath(this.host);
    }

    public Path getSessionPath(String host) {
        return sessionFolder.resolve(host + SESSION_FILENAME_SUFFIX);
    }

    public Session getSession() {
        return getSession(this.host);
    }

    public Session getSession(String host) {
        Path sessionPath = sessionFolder.resolve(host + SESSION_FILENAME_SUFFIX);
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


    public void updateSessionToken(String token, String host) throws IOException {
        updateSessionToken(token, host, Collections.emptyMap());
    }

    public void updateSessionToken(String token, String host, Map<String, Object> attributes) throws IOException {
        // Get current Session and update token
        Session session = getSession(host);
        session.setToken(token);
        session.setAttributes(attributes);

        // Save updated Session
        saveSession(session);
    }

    public void saveSession(String user, String token, String refreshToken, List<String> studies, String host) throws IOException {
        saveSession(user, token, refreshToken, studies, host, Collections.emptyMap());
    }

    public void saveSession(String user, String token, String refreshToken, List<String> studies, String host,
                            Map<String, Object> attributes) throws IOException {
        Session session = new Session(host, user, token, refreshToken, studies, attributes);
        if (CollectionUtils.isNotEmpty(studies)) {
            session.setCurrentStudy(studies.get(0));
        } else {
            session.setCurrentStudy(NO_STUDY);
        }
        saveSession(session, host);
    }

    public void refreshSession(String refreshToken, String host)
            throws IOException {
        Session session = getSession().setRefreshToken(refreshToken);

        saveSession(session, host);
    }

    public void saveSession() throws IOException {
        saveSession(getSession(), host);
    }

    public void saveSession(Session session) throws IOException {
        saveSession(session, host);
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
            this.saveSession(user, response.getToken(), response.getRefreshToken(), studies, this.host, attributes);
            res.setType(QueryType.VOID);
        }
        return res;
    }

    public RestResponse<AuthenticationResponse> saveSession(String user, AuthenticationResponse response, OpenCGAClient openCGAClient)
            throws ClientException, IOException {
        return saveSession(user, response, openCGAClient, Collections.emptyMap());
    }

    public void saveSession(Session session, String host) throws IOException {
        // Check if ~/.opencga folder exists
        if (!Files.exists(sessionFolder)) {
            Files.createDirectory(sessionFolder);
        }
        logger.debug("Saving '{}'", session);
        logger.debug("Session file '{}'", host + SESSION_FILENAME_SUFFIX);

        Path sessionPath = sessionFolder.resolve(host + SESSION_FILENAME_SUFFIX);
        objectWriter.writeValue(sessionPath.toFile(), session);
    }

    public void logoutSessionFile() throws IOException {
        // We just need to save an empty session, this will delete user and token for this host
        logger.debug("Session logout for host '{}'", host);
        saveSession(createEmptySession(), host);
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SessionManager{");
        sb.append("clientConfiguration=").append(clientConfiguration);
        sb.append(", host='").append(host).append('\'');
        sb.append(", Session='").append(getSession().toString()).append('\'');
        sb.append('}');
        return sb.toString();
    }


}
