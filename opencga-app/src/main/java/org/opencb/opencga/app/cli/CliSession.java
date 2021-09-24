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

package org.opencb.opencga.app.cli;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Created by imedina on 13/07/15.
 */
public class CliSession {

    private String host;
    private String version;
    private String user;
    private String token;
    private String refreshToken;
    private String login;
    private String expirationTime;
    private List<String> studies;
    private String currentStudy;

    private static final String SESSION_FILENAME = "session.json";
    private Logger privateLogger= LoggerFactory.getLogger(CommandExecutor.class);

    private static CliSession instance;
    private CliSession(){}

    public static CliSession getInstance(){
       if(instance==null){
           loadCliSessionFile();
       }
        return instance;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CliSession{");
        sb.append("host='").append(host).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", refreshToken='").append(refreshToken).append('\'');
        sb.append(", login='").append(login).append('\'');
        sb.append(", expirationTime='").append(expirationTime).append('\'');
        sb.append(", studies=").append(studies);
        sb.append('}');
        return sb.toString();
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

    public String getExpirationTime() {
        return expirationTime;
    }

    public CliSession setExpirationTime(String expirationTime) {
        this.expirationTime = expirationTime;
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


    private static void loadCliSessionFile() {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            try {
                instance = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .readValue(sessionPath.toFile(), CliSession.class);
            } catch (IOException e) {
                System.err.println("Could not parse the session file properly");
                e.printStackTrace();
            }
        }
    }

    public void saveCliSessionFile() throws IOException {
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
        sessionPath = sessionPath.resolve(SESSION_FILENAME);



        // we remove the part where the token signature is to avoid key verification
        int i = token.lastIndexOf('.');
        String withoutSignature = token.substring(0, i+1);
        Date expiration = Jwts.parser().parseClaimsJwt(withoutSignature).getBody().getExpiration();

        instance.setExpirationTime(TimeUtils.getTime(expiration));

        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), instance);
    }

    public void updateCliSessionFile() throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(sessionPath.toFile(), instance);
        }
    }


    public void logoutCliSessionFile() throws IOException {
        Path sessionPath = Paths.get(System.getProperty("user.home"), ".opencga", SESSION_FILENAME);
        if (Files.exists(sessionPath)) {
            Files.delete(sessionPath);
        }
    }

    public boolean isValid() {

        if(Long.parseLong(TimeUtils.getTime(new Date())) > Long.parseLong(instance.getExpirationTime())){
            return false;
        }
        return true;
    }
}
