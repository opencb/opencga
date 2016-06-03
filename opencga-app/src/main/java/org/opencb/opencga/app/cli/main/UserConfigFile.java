package org.opencb.opencga.app.cli.main;

/**
 * Created by hpccoll1 on 13/07/15.
 */
public class UserConfigFile {

    private String userId;
    private String sessionId;
    private long timestamp;

    public UserConfigFile(String userId, String sessionId) {
        this(userId, sessionId, System.currentTimeMillis());
    }

    public UserConfigFile(String userId, String sessionId, long timestamp) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserConfigFile{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", sessionId='").append(sessionId).append('\'');
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
