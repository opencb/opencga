package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class Container {

    @DataField(id = "name", description = FieldConstants.CONTAINER_NAME_DESCRIPTION)
    private String name;

    @DataField(id = "tag", description = FieldConstants.CONTAINER_TAG_DESCRIPTION)
    private String tag;

    @DataField(id = "digest", description = FieldConstants.CONTAINER_DIGEST_DESCRIPTION)
    private String digest;

    @DataField(id = "commandLine", description = FieldConstants.CONTAINER_COMMANDLINE_DESCRIPTION)
    private String commandLine;

    @DataField(id = "user", description = FieldConstants.CONTAINER_USER_DESCRIPTION)
    private String user;

    @DataField(id = "password", description = FieldConstants.CONTAINER_PASSWORD_DESCRIPTION)
    private String password;

    public Container() {
    }

    public Container(String name, String tag, String digest, String commandLine, String user, String password) {
        this.name = name;
        this.tag = tag;
        this.digest = digest;
        this.commandLine = commandLine;
        this.user = user;
        this.password = password;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Container{");
        sb.append("name='").append(name).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", digest='").append(digest).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", user='").append("xxxxxxxxxx").append('\'');
        sb.append(", password='").append("xxxxxxxxxx").append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public Container setName(String name) {
        this.name = name;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public Container setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public String getDigest() {
        return digest;
    }

    public Container setDigest(String digest) {
        this.digest = digest;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public Container setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getUser() {
        return user;
    }

    public Container setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Container setPassword(String password) {
        this.password = password;
        return this;
    }
}
