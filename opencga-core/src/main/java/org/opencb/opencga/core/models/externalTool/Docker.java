package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class Docker {

    @DataField(id = "name", description = FieldConstants.DOCKER_NAME_DESCRIPTION)
    private String name;

    @DataField(id = "tag", description = FieldConstants.DOCKER_TAG_DESCRIPTION)
    private String tag;

    @DataField(id = "commandLine", description = FieldConstants.DOCKER_COMMANDLINE_DESCRIPTION)
    private String commandLine;

    @DataField(id = "user", description = FieldConstants.DOCKER_USER_DESCRIPTION)
    private String user;

    @DataField(id = "password", description = FieldConstants.DOCKER_PASSWORD_DESCRIPTION)
    private String password;

    public Docker() {
    }

    public Docker(String name, String tag, String commandLine, String user, String password) {
        this.name = name;
        this.tag = tag;
        this.commandLine = commandLine;
        this.user = user;
        this.password = password;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Docker{");
        sb.append("name='").append(name).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public Docker setName(String name) {
        this.name = name;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public Docker setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public Docker setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public String getUser() {
        return user;
    }

    public Docker setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Docker setPassword(String password) {
        this.password = password;
        return this;
    }
}
