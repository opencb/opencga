package com.zettagenomics.opencga.enterprise.core.models.federation;

import java.util.List;

public class FederationUserParams {

    private List<String> userIds;

    public FederationUserParams() {
    }

    public FederationUserParams(List<String> userIds) {
        this.userIds = userIds;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationUserParams{");
        sb.append("userIds=").append(userIds);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getUserIds() {
        return userIds;
    }

    public FederationUserParams setUserIds(List<String> userIds) {
        this.userIds = userIds;
        return this;
    }
}
