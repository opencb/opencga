package org.opencb.opencga.core.models.user;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class AuthenticationResponse {

    @DataField(description = ParamConstants.AUTHENTICATION_RESPONSE_TOKEN_DESCRIPTION)
    private String token;
    @DataField(description = ParamConstants.AUTHENTICATION_RESPONSE_REFRESH_TOKEN_DESCRIPTION)
    private String refreshToken;

    public AuthenticationResponse() {
    }

    public AuthenticationResponse(String token) {
        this.token = token;
        this.refreshToken = token;
    }

    public AuthenticationResponse(String token, String refreshToken) {
        this.token = token;
        this.refreshToken = refreshToken;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AuthenticationResponse{");
        sb.append("token='").append(token).append('\'');
        sb.append(", refreshToken='").append(refreshToken).append('\'');
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
}
