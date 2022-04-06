package org.opencb.opencga.test.config;

import java.util.List;

public class Aligner {

    private String name;
    private String image;
    private String command;
    private List<String> params;
    private boolean skip;

    public Aligner() {

    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Aligner{\n");
        sb.append("name='").append(name).append('\'').append("\n");
        sb.append("image='").append(image).append('\'').append("\n");
        sb.append("command='").append(command).append('\'').append("\n");
        sb.append("params=").append(params).append("\n");
        sb.append("skip=").append(skip).append("\n");
        sb.append("}\n");
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public Aligner setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isSkip() {
        return skip;
    }

    public Aligner setSkip(boolean skip) {
        this.skip = skip;
        return this;
    }

    public String getImage() {
        return image;
    }

    public Aligner setImage(String image) {
        this.image = image;
        return this;
    }

    public String getCommand() {
        return command;
    }

    public Aligner setCommand(String command) {
        this.command = command;
        return this;
    }

    public List<String> getParams() {
        return params;
    }

    public Aligner setParams(List<String> params) {
        this.params = params;
        return this;
    }
}
