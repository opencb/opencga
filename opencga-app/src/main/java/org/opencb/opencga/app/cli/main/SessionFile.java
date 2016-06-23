/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.app.cli.main;

import org.joda.time.Instant;

import java.time.LocalDateTime;

/**
 * Created by imedina on 13/07/15.
 */
public class SessionFile {

    private String userId;
    private String sessionId;
    private String login;
    private String logout;
    private long timestamp;


    public SessionFile() {
    }

    public SessionFile(String userId, String sessionId) {
        this(userId, sessionId, LocalDateTime.now().toString(), null, Instant.now().getMillis());
    }

    public SessionFile(String userId, String sessionId, String login, String logout, long timestamp) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.login = login;
        this.logout = logout;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserConfigFile{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", sessionId='").append(sessionId).append('\'');
        sb.append(", login=").append(login);
        sb.append(", logout=").append(logout);
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }

    public String getUserId() {
        return userId;
    }

    public SessionFile setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public SessionFile setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public String getLogin() {
        return login;
    }

    public SessionFile setLogin(String login) {
        this.login = login;
        return this;
    }

    public String getLogout() {
        return logout;
    }

    public SessionFile setLogout(String logout) {
        this.logout = logout;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public SessionFile setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }
}
