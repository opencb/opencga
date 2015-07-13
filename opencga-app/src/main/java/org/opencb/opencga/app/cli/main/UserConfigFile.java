package org.opencb.opencga.app.cli.main;

/**
 * Created by hpccoll1 on 13/07/15.
 */
public class UserConfigFile {
    String userId;
    String sessionId;

    public UserConfigFile() {
    }

    public UserConfigFile(String userId, String sessionId) {
        this.userId = userId;
        this.sessionId = sessionId;
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
}
