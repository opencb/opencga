package org.opencb.opencga.core.models.wrapper.hisat2;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.List;

public class Hisat2Params {

    protected String command;
    protected List<Object> input;
    protected ObjectMap params;

    public Hisat2Params() {
        this("hisat2", new ArrayList<>(), new ObjectMap());
    }

    public Hisat2Params(String command, List<Object> input, ObjectMap params) {
        this.command = command;
        this.input = input;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Hisat2Params{");
        sb.append("command='").append(command).append('\'');
        sb.append(", input=").append(input);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public Hisat2Params setCommand(String command) {
        this.command = command;
        return this;
    }

    public List<Object> getInput() {
        return input;
    }

    public Hisat2Params setInput(List<Object> input) {
        this.input = input;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public Hisat2Params setParams(ObjectMap params) {
        this.params = params;
        return this;
    }
}
