package org.opencb.opencga.app.demo.config;

import java.util.List;

public class Configuration {

    private String password;
    private List<StudyConfiguration> studies;

    public Configuration() {
    }

    public Configuration(List<StudyConfiguration> studies) {
        this.studies = studies;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration{");
        sb.append("password='").append(password).append('\'');
        sb.append(", studies=").append(studies);
        sb.append('}');
        return sb.toString();
    }

    public String getPassword() {
        return password;
    }

    public Configuration setPassword(String password) {
        this.password = password;
        return this;
    }

    public List<StudyConfiguration> getStudies() {
        return studies;
    }

    public Configuration setStudies(List<StudyConfiguration> studies) {
        this.studies = studies;
        return this;
    }
}
