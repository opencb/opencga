package org.opencb.opencga.test.config;

import java.util.List;

public class Caller {


    private String name;
    private String image;
    private String command;
    private boolean skip;
    private List<String> params;

    public Caller() {

    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Caller{\n");
        sb.append("name='").append(name).append('\'').append("\n");
        sb.append("image='").append(image).append('\'').append("\n");
        sb.append("command='").append(command).append('\'').append("\n");
        sb.append("params=").append(params).append("\n");
        sb.append("skip=").append(skip).append("\n");
        sb.append("}\n");
        return sb.toString();
    }

    public boolean isSkip() {
        return skip;
    }

    public Caller setSkip(boolean skip) {
        this.skip = skip;
        return this;
    }

    public String getName() {
        return name;
    }

    public Caller setName(String name) {
        this.name = name;
        return this;
    }

    public String getImage() {
        return image;
    }

    public Caller setImage(String image) {
        this.image = image;
        return this;
    }

    public String getCommand() {
        return command;
    }

    public Caller setCommand(String command) {
        this.command = command;
        return this;
    }

    public List<String> getParams() {
        return params;
    }

    public Caller setParams(List<String> params) {
        this.params = params;
        return this;
    }
}
