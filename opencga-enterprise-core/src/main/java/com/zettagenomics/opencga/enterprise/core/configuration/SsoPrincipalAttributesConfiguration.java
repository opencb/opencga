package com.zettagenomics.opencga.enterprise.core.configuration;

public class SsoPrincipalAttributesConfiguration {

    private String name;
    private String surname;
    private String email;
    private String groups;

    public SsoPrincipalAttributesConfiguration() {
    }

    public SsoPrincipalAttributesConfiguration(String name, String surname, String email, String groups) {
        this.name = name;
        this.surname = surname;
        this.email = email;
        this.groups = groups;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SsoPrincipalAttributesConfiguration{");
        sb.append("name='").append(name).append('\'');
        sb.append(", surname='").append(surname).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", groups='").append(groups).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public SsoPrincipalAttributesConfiguration setName(String name) {
        this.name = name;
        return this;
    }

    public String getSurname() {
        return surname;
    }

    public SsoPrincipalAttributesConfiguration setSurname(String surname) {
        this.surname = surname;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public SsoPrincipalAttributesConfiguration setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getGroups() {
        return groups;
    }

    public SsoPrincipalAttributesConfiguration setGroups(String groups) {
        this.groups = groups;
        return this;
    }

}
