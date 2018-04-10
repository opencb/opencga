package org.opencb.opencga.core.models.clinical;

public class Panel {

    private String name;
    private String version;
    private String date;

    public Panel(String name, String version, String date) {
        this.name = name;
        this.version = version;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Panel{");
        sb.append("name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public Panel setName(String name) {
        this.name = name;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public Panel setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getDate() {
        return date;
    }

    public Panel setDate(String date) {
        this.date = date;
        return this;
    }
}
