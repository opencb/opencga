package org.opencb.opencga.core.models;

public class AuthenticationResponse {

    private String token;
    private String refreshToken;

    @Deprecated
    private String sessionId;

    @Deprecated
    private String id;

    public AuthenticationResponse() {
    }

    public AuthenticationResponse(String token) {
        this.token = token;
        this.sessionId = token;
        this.id = token;
        this.refreshToken = token;
    }

    public AuthenticationResponse(String token, String refreshToken) {
        this.token = token;
        this.sessionId = token;
        this.id = token;
        this.refreshToken = refreshToken;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AuthenticationResponse{");
        sb.append("token='").append(token).append('\'');
        sb.append(", refreshToken='").append(refreshToken).append('\'');
        sb.append(", sessionId='").append(sessionId).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getToken() {
        return token;
    }

    public AuthenticationResponse setToken(String token) {
        this.token = token;
        return this;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public AuthenticationResponse setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public AuthenticationResponse setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public String getId() {
        return id;
    }

    public AuthenticationResponse setId(String id) {
        this.id = id;
        return this;
    }
}
