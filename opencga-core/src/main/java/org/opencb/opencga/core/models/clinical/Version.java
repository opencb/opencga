package org.opencb.opencga.core.models.clinical;

public class Version {

    private String id;
    private String name;
    private String type;
    private String version;

    public Version() {
    }

    public Version(String id, String name, String type, String version) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Version{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Version setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Version setName(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public Version setType(String type) {
        this.type = type;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public Version setVersion(String version) {
        this.version = version;
        return this;
    }

}
