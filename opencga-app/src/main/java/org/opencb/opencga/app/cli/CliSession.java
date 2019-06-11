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

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 13/07/15.
 */
public class CliSession {

    private String host;
    private String version;
    private String user;
    private String token;
    private String login;
    private String expirationTime;
    @Deprecated
    private Map<String, List<String>> projectsAndStudies;
    private List<String> studies;

    public CliSession() {
    }

    public CliSession(String host, String user, String token) {
        this(host, user, token, TimeUtils.getTime(), Collections.emptyList());
    }

    public CliSession(String host, String user, String token, List<String> studies) {
        this(host, user, token, TimeUtils.getTime(), studies);
    }

    public CliSession(String host, String user, String token, String login, List<String> studies) {
        this.host = host;
        this.user = user;
        this.token = token;
        this.login = login;
        this.studies = studies;
        this.version = "v1";
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CliSession{");
        sb.append("host='").append(host).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", login='").append(login).append('\'');
        sb.append(", expirationTime='").append(expirationTime).append('\'');
        sb.append(", projectsAndStudies=").append(projectsAndStudies);
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

    @Deprecated
    public Map<String, List<String>> getProjectsAndStudies() {
        return projectsAndStudies;
    }

    @Deprecated
    public CliSession setProjectsAndStudies(Map<String, List<String>> projectsAndStudies) {
        this.projectsAndStudies = projectsAndStudies;
        return this;
    }

    public List<String> getStudies() {
        return studies;
    }

    public void setStudies(List<String> studies) {
        this.studies = studies;
    }
}
