package org.opencb.opencga.client.template.config;

import org.opencb.opencga.core.models.file.FileUpdateParams;

public class TemplateFile extends FileUpdateParams {

    private String path;
    private String uri;

    public String getPath() {
        return path;
    }

    public TemplateFile setPath(String path) {
        this.path = path;
        return this;
    }

    public String getUri() {
        return uri;
    }

    public TemplateFile setUri(String uri) {
        this.uri = uri;
        return this;
    }
}
