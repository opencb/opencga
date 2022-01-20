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
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SessionManager {

    public static final String SESSION_FILENAME_SUFFIX = "_session.json";
    private static final String NO_TOKEN = "NO_TOKEN";
    private static final String NO_STUDY = "NO_STUDY";
    private static final String ANONYMOUS = "anonymous";
    private final ClientConfiguration clientConfiguration;
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
//        boolean validHost = false;
//        for (HostConfig hostConfig : clientConfiguration.getRest().getHosts()) {
//            if (hostConfig.getName().equals(host)) {
//                validHost = true;
//                break;
//            }
//        }

        // Prepare objects for writing and reading sessions
        ObjectMapper objectMapper = new ObjectMapper();
        this.objectWriter = objectMapper
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .writerFor(CliSession.class)
                .withDefaultPrettyPrinter();
        this.objectReader = objectMapper
                .readerFor(CliSession.class);
    }

    private CliSession createEmptySession() {
        CliSession clisession = new CliSession();
        clisession.setHost(host);
        clisession.setCurrentStudy(NO_STUDY);
        clisession.setToken(NO_TOKEN);
        clisession.setUser(ANONYMOUS);
        return clisession;
    }

    public Path getCliSessionPath() {
        return getCliSessionPath(this.host);
    }

    public Path getCliSessionPath(String host) {
        return sessionFolder.resolve(host + SESSION_FILENAME_SUFFIX);
    }

    public CliSession getCliSession() {
        return getCliSession(this.host);
    }

    public CliSession getCliSession(String host) {
        Path sessionPath = sessionFolder.resolve(host + SESSION_FILENAME_SUFFIX);
        if (Files.exists(sessionPath)) {
            try {
                return objectReader.readValue(sessionPath.toFile());
            } catch (IOException e) {
                logger.debug("Could not parse the session file properly");
            }
        }

        CliSession clisession = createEmptySession();
        try {
            saveCliSession(clisession);
        } catch (IOException e) {
            logger.debug("Could not create the session file properly");
        }
        return clisession;
    }

    public void updateSessionToken(String token) throws IOException {
        updateSessionToken(token, host);
    }

    public void updateSessionToken(String token, String host) throws IOException {
        // Get current Session and update token
        CliSession cliSession = getCliSession(host);
        cliSession.setToken(token);

        // Save updated Session
        saveCliSession(cliSession);
    }

    public void saveCliSession(String user, String token, String refreshToken, List<String> studies, String host)
            throws IOException {
        CliSession cliSession = new CliSession(host, user, token, refreshToken, studies);
        if (CollectionUtils.isNotEmpty(studies)) {
            cliSession.setCurrentStudy(studies.get(0));
        } else {
            cliSession.setCurrentStudy(NO_STUDY);
        }
        saveCliSession(cliSession, host);
    }

    public void saveCliSession() throws IOException {
        saveCliSession(getCliSession(), host);
    }

    public void saveCliSession(CliSession cliSession) throws IOException {
        saveCliSession(cliSession, host);
    }

    public void saveCliSession(CliSession cliSession, String host) throws IOException {
        // Check if ~/.opencga folder exists
        if (!Files.exists(sessionFolder)) {
            Files.createDirectory(sessionFolder);
        }

        Path sessionPath = sessionFolder.resolve(host + SESSION_FILENAME_SUFFIX);
        objectWriter.writeValue(sessionPath.toFile(), cliSession);
    }

    public void logoutCliSessionFile() throws IOException {
        // We just need to save an empty session, this will delete user and token for this host
        logger.debug("Session logout for host '{}'", host);
        saveCliSession(createEmptySession(), host);
    }


    public boolean hasSessionToken() {
        return !StringUtils.isEmpty(getToken()) && !SessionManager.NO_TOKEN.equals(getToken());
    }

    // Wrappers to getters

    public String getHost() {
        return host;
    }

    public String getUser() {
        return getCliSession().getUser();
    }

    public String getToken() {
        return getCliSession().getToken();
    }

    public String getRefreshToken() {
        return getCliSession().getRefreshToken();
    }

    public String getVersion() {
        return getCliSession().getVersion();
    }

    public String getLogin() {
        return getCliSession().getLogin();
    }

    public List<String> getStudies() {
        return getCliSession().getStudies();
    }

    public long getTimestamp() {
        return getCliSession().getTimestamp();
    }

    public String getCurrentStudy() {
        return getCliSession().getCurrentStudy();
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SessionManager{");
        sb.append("clientConfiguration=").append(clientConfiguration);
        sb.append(", host='").append(host).append('\'');
        sb.append(", Session='").append(getCliSession().toString()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
