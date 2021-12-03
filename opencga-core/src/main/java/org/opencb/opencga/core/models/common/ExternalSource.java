package org.opencb.opencga.core.models.common;

public class ExternalSource {

    private String id;
    private String name;
    private String description;
    private String source;
    private String url;

    public ExternalSource() {
    }

    public ExternalSource(String id, String name, String description, String source, String url) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.source = source;
        this.url = url;
    }

    public static ExternalSource init() {
        return new ExternalSource("", "", "", "", "");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExternalSource{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ExternalSource setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ExternalSource setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExternalSource setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getSource() {
        return source;
    }

    public ExternalSource setSource(String source) {
        this.source = source;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public ExternalSource setUrl(String url) {
        this.url = url;
        return this;
    }
}
