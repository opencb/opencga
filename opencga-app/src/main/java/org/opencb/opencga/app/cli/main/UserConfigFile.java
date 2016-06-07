package org.opencb.opencga.app.cli.main;

import org.joda.time.Instant;

import java.time.LocalDateTime;

/**
 * Created by hpccoll1 on 13/07/15.
 */
public class UserConfigFile {

    private String userId;
    private String sessionId;
    private String login;
    private String logout;
    private long timestamp;


    public UserConfigFile() {
    }

    public UserConfigFile(String userId, String sessionId) {
        this(userId, sessionId, LocalDateTime.now().toString(), Instant.now().getMillis());
    }

    public UserConfigFile(String userId, String sessionId, String login, long timestamp) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.login = login;
        this.timestamp = timestamp;
        System.out.println("timestamp = " + timestamp);
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

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getLogin() {
        return login;
    }

    public UserConfigFile setLogin(String login) {
        this.login = login;
        return this;
    }

    public String getLogout() {
        return logout;
    }

    public UserConfigFile setLogout(String logout) {
        this.logout = logout;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public UserConfigFile setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }
}
