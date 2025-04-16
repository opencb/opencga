package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class Docker {

    @DataField(id = "id", description = FieldConstants.DOCKER_ID_DESCRIPTION)
    private String id;

    @DataField(id = "tag", description = FieldConstants.DOCKER_TAG_DESCRIPTION)
    private String tag;

    @DataField(id = "author", description = FieldConstants.DOCKER_AUTHOR_DESCRIPTION)
    private String author;

    @DataField(id = "description", description = FieldConstants.DOCKER_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "cli", description = FieldConstants.DOCKER_CLI_DESCRIPTION)
    private String cli;

    public Docker() {
    }

    public Docker(String id, String tag, String author, String description, String cli) {
        this.id = id;
        this.tag = tag;
        this.author = author;
        this.description = description;
        this.cli = cli;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExternalToolDocker{");
        sb.append("id='").append(id).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", author='").append(author).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", cli='").append(cli).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Docker setId(String id) {
        this.id = id;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public Docker setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public String getAuthor() {
        return author;
    }

    public Docker setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Docker setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCli() {
        return cli;
    }

    public Docker setCli(String cli) {
        this.cli = cli;
        return this;
    }
}
