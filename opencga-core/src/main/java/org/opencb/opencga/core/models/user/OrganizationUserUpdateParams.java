package org.opencb.opencga.core.models.user;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class OrganizationUserUpdateParams extends UserUpdateParams {

    private UserQuota quota;
    private Account account;
    private Map<String, Object> attributes;

    public OrganizationUserUpdateParams() {
    }

    public OrganizationUserUpdateParams(String name, String email, UserQuota quota, Account account, Map<String, Object> attributes) {
        super(name, email);
        this.quota = quota;
        this.account = account;
        this.attributes = attributes;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationUserUpdateParams{");
        sb.append("quota=").append(quota);
        sb.append(", account=").append(account);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public UserQuota getQuota() {
        return quota;
    }

    public OrganizationUserUpdateParams setQuota(UserQuota quota) {
        this.quota = quota;
        return this;
    }

    public Account getAccount() {
        return account;
    }

    public OrganizationUserUpdateParams setAccount(Account account) {
        this.account = account;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public OrganizationUserUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    @Override
    public OrganizationUserUpdateParams setName(String name) {
        super.setName(name);
        return this;
    }

    @Override
    public OrganizationUserUpdateParams setEmail(String email) {
        super.setEmail(email);
        return this;
    }

    public static class Account {
        private String expirationDate;

        public Account() {
        }

        public Account(String expirationDate) {
            this.expirationDate = expirationDate;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Account{");
            sb.append("expirationDate='").append(expirationDate).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getExpirationDate() {
            return expirationDate;
        }

        public Account setExpirationDate(String expirationDate) {
            this.expirationDate = expirationDate;
            return this;
        }
    }

}
