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

import java.util.List;

/**
 * Created by imedina on 13/07/15.
 */
public class Session {

    private String host;
    private String user;
    private String token;
    private String refreshToken;
    private String version;
    private String login;
    private List<String> studies;
    private long timestamp;
    private String currentStudy;

    public Session() {
    }

    public Session(String host, String user, String token, String refreshToken, List<String> studies) {
        this.host = host;
        this.user = user;
        this.token = token;
        this.refreshToken = refreshToken;
        this.studies = studies;
    }

    public Session(String host, String user, String token, String refreshToken, String version, String login, List<String> studies, long timestamp, String currentStudy) {
        this.host = host;
        this.user = user;
        this.token = token;
        this.refreshToken = refreshToken;
        this.version = version;
        this.login = login;
        this.studies = studies;
        this.timestamp = timestamp;
        this.currentStudy = currentStudy;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CliSession{");
        sb.append("host='").append(host).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", refreshToken='").append(refreshToken).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", login='").append(login).append('\'');
        sb.append(", studies=").append(studies);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", currentStudy='").append(currentStudy).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getHost() {
        return host;
    }

    public Session setHost(String host) {
        this.host = host;
        return this;
    }

    public String getUser() {
        return user;
    }

    public Session setUser(String user) {
        this.user = user;
        return this;
    }

    public String getToken() {
        return token;
    }

    public Session setToken(String token) {
        this.token = token;
        return this;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public Session setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public Session setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getLogin() {
        return login;
    }

    public Session setLogin(String login) {
        this.login = login;
        return this;
    }

    public List<String> getStudies() {
        return studies;
    }

    public Session setStudies(List<String> studies) {
        this.studies = studies;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Session setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String getCurrentStudy() {
        return currentStudy;
    }

    public Session setCurrentStudy(String currentStudy) {
        this.currentStudy = currentStudy;
        return this;
    }
}
