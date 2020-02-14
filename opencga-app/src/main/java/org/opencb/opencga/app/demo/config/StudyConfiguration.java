package org.opencb.opencga.app.demo.config;

public class StudyConfiguration {

    private String id;
    private String index;
    private String urlBase;
    private boolean active;

    public StudyConfiguration() {
    }

    public StudyConfiguration(String id, String index, String urlBase, boolean active) {
        this.id = id;
        this.index = index;
        this.urlBase = urlBase;
        this.active = active;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", index='").append(index).append('\'');
        sb.append(", urlBase='").append(urlBase).append('\'');
        sb.append(", active='").append(active).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public StudyConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public String getIndex() {
        return index;
    }

    public StudyConfiguration setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getUrlBase() {
        return urlBase;
    }

    public StudyConfiguration setUrlBase(String urlBase) {
        this.urlBase = urlBase;
        return this;
    }

    public boolean getActive() {
        return active;
    }

    public StudyConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }
}
