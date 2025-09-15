package org.opencb.opencga.core.config;

public class UserOrganizationConfiguration {

    private String defaultExpirationDate;
    private boolean addToStudyMembers;

    public UserOrganizationConfiguration() {
    }

    public UserOrganizationConfiguration(String defaultExpirationDate, boolean addToStudyMembers) {
        this.defaultExpirationDate = defaultExpirationDate;
        this.addToStudyMembers = addToStudyMembers;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserOrganizationConfiguration{");
        sb.append("defaultExpirationDate='").append(defaultExpirationDate).append('\'');
        sb.append(", addToStudyMembers=").append(addToStudyMembers);
        sb.append('}');
        return sb.toString();
    }

    public String getDefaultExpirationDate() {
        return defaultExpirationDate;
    }

    public UserOrganizationConfiguration setDefaultExpirationDate(String defaultExpirationDate) {
        this.defaultExpirationDate = defaultExpirationDate;
        return this;
    }

    public boolean isAddToStudyMembers() {
        return addToStudyMembers;
    }

    public UserOrganizationConfiguration setAddToStudyMembers(boolean addToStudyMembers) {
        this.addToStudyMembers = addToStudyMembers;
        return this;
    }
}
