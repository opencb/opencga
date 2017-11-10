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

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 13/07/15.
 */
public class CliSession {

    private String userId;
    private String token;
    private String login;
    private String logout;
    private Map<String, List<String>> projectsAndStudies;

    public CliSession() {
    }

    public CliSession(String userId, String token) {
        this(userId, token, LocalDateTime.now().toString(), null, new HashMap<>());
    }

    public CliSession(String userId, String token, Map<String, List<String>> projectsAndStudies) {
        this(userId, token, LocalDateTime.now().toString(), null, projectsAndStudies);
    }

    public CliSession(String userId, String token, String login, String logout, Map<String, List<String>> projectsAndStudies) {
        this.userId = userId;
        this.token = token;
        this.login = login;
        this.logout = logout;
        this.projectsAndStudies = projectsAndStudies;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CliSession{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", sessionId='").append(token).append('\'');
        sb.append(", login='").append(login).append('\'');
        sb.append(", logout='").append(logout).append('\'');
        sb.append(", projects=").append(projectsAndStudies);
        sb.append('}');
        return sb.toString();
    }

    public String getUserId() {
        return userId;
    }

    public CliSession setUserId(String userId) {
        this.userId = userId;
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

    public String getLogout() {
        return logout;
    }

    public CliSession setLogout(String logout) {
        this.logout = logout;
        return this;
    }

    public Map<String, List<String>> getProjectsAndStudies() {
        return projectsAndStudies;
    }

    public CliSession setProjectsAndStudies(Map<String, List<String>> projectsAndStudies) {
        this.projectsAndStudies = projectsAndStudies;
        return this;
    }
}
