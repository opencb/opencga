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

package org.opencb.opencga.app.cli.session;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 13/07/15.
 */
public class CliSession {

    public static final String GUEST_USER = "anonymous";
    public static final String NO_STUDY = "NO_STUDY";
    private final long timestamp;
    private String host;
    private String version;
    private String user;
    private String token;
    private String refreshToken;
    private String login;
    private List<String> studies;
    private String currentStudy;
    private String currentHost;

    public static final String SESSION_FILENAME = "Session.json";
    private Logger privateLogger = LoggerFactory.getLogger(CommandExecutor.class);

    private static CliSession instance;

    private CliSession() {
        ClientConfiguration.getInstance().loadClientConfiguration();
        host = "localhost";
        version = "-1";
        user = GUEST_USER;
        token = "";
        refreshToken = "";
        login = "19740927121845";
        currentStudy = NO_STUDY;
        studies = Collections.emptyList();
        currentHost = ClientConfiguration.getInstance().getRest().getHostname();
        timestamp = System.currentTimeMillis();
    }

    static CliSession getInstance() {
        if (instance == null) {
            loadCliSessionFile(getLastHostUsed());
        }
        return instance;
    }

    public static String getLastHostUsed() {
        Path sessionDir = Paths.get(System.getProperty("user.home"), ".opencga");
        if (!Files.exists(sessionDir)) {
            return "";
        }
        Map<String, Long> mapa = new HashMap<String, Long>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sessionDir)) {
            for (Path path : stream) {
                if (!Files.isDirectory(path)) {
                    if (path.endsWith(CliSession.SESSION_FILENAME)) {
                        CliSession cli = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                                .readValue(path.toFile(), CliSession.class);
                        mapa.put(cli.getCurrentHost(), cli.getTimestamp());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String res = "";
        Long max = new Long(0);
        for (Map.Entry entry : mapa.entrySet()) {
            if (((Long) entry.getValue()) > max) {
                res = String.valueOf(entry.getKey());
            }
        }
        return res;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CliSession{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", host='").append(host).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", refreshToken='").append(refreshToken).append('\'');
        sb.append(", login='").append(login).append('\'');
        sb.append(", studies=").append(studies);
        sb.append(", currentStudy='").append(currentStudy).append('\'');
        sb.append(", currentHost='").append(currentHost).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static void loadCliSessionFile(String host) {
        Path sessionDir = Paths.get(System.getProperty("user.home"), ".opencga");
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", host + SESSION_FILENAME);

        if (!Files.exists(sessionDir)) {
            try {
                Files.createDirectory(sessionDir);
            } catch (Exception e) {
                OpencgaMain.printErrorMessage("Could not create session dir properly", e);
            }
        }
        if (!Files.exists(sessionPath)) {
            try {
                Files.createFile(sessionPath);
                instance = new CliSession();
                updateCliSessionFile(host);
            } catch (Exception e) {
                OpencgaMain.printErrorMessage("Could not create session file properly", e);
            }
        } else {
            try {
                instance = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .readValue(sessionPath.toFile(), CliSession.class);
            } catch (IOException e) {
                OpencgaMain.printErrorMessage("Could not parse the session file properly", e);
            }
        }
    }

    public void saveCliSessionFile(String host) throws IOException {
        // Check the home folder exists
        if (!Files.exists(Paths.get(System.getProperty("user.home")))) {
            System.out.println("WARNING: Could not store token. User home folder '" + System.getProperty("user.home")
                    + "' not found. Please, manually provide the token for any following command lines with '-S {token}'.");
            return;
        }

        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga");
        // check if ~/.opencga folder exists
        if (!Files.exists(sessionPath)) {
            Files.createDirectory(sessionPath);
        }
        sessionPath = sessionPath.resolve(host + SESSION_FILENAME);

        // we remove the part where the token signature is to avoid key verification
      /*  if (StringUtils.isNotEmpty(token) &&) {
            int i = token.lastIndexOf('.');
            String withoutSignature = token.substring(0, i + 1);
            Date expiration = Jwts.parser().parseClaimsJwt(withoutSignature).getBody().getExpiration();
            instance.setExpirationTime(TimeUtils.getTime(expiration));
        }*/
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), instance);
    }

    public static void updateCliSessionFile(String host) throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", host + SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), instance);
        }
    }

    public void logoutCliSessionFile(String host) throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", host + SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            Files.delete(sessionPath);
        }
        instance = null;
    }

    public String getHost() {
        return host;
    }

    public CliSession setHost(String host) {
        this.host = host;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public CliSession setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getUser() {
        return user;
    }

    public CliSession setUser(String user) {
        this.user = user;
        return this;
    }

    public String getToken() {
        return token;
    }

    public CliSession setToken(String token) {
        this.token = token;
        return this;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public CliSession setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
        return this;
    }

    public String getLogin() {
        return login;
    }

    public CliSession setLogin(String login) {
        this.login = login;
        return this;
    }

    public List<String> getStudies() {
        return studies;
    }

    public void setStudies(List<String> studies) {
        this.studies = studies;
    }

    public String getCurrentStudy() {
        return currentStudy;
    }

    public CliSession setCurrentStudy(String currentStudy) {
        this.currentStudy = currentStudy;
        return this;
    }

    public String getCurrentHost() {
        return currentHost;
    }

    public CliSession setCurrentHost(String currentHost) {
        this.currentHost = currentHost;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
