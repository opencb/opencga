package org.opencb.opencga.core.models.wrapper;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WrapperParams {

    private String command;
    private List<String> input;
    private ObjectMap options;

    public WrapperParams() {
        this("", new ArrayList<>(), new ObjectMap());
    }

    public WrapperParams(String command, List<String> input, ObjectMap options) {
        this.command = command;
        this.input = input;
        this.options = options;
    }

    public static <T extends WrapperParams> WrapperParams copy(T src) {
        return new WrapperParams(src.getCommand(), new ArrayList<>(src.getInput()), new ObjectMap(src.getOptions()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WrapperParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", input=").append(input);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getCommand() {
        return command;
    }

    public WrapperParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public List<String> getInput() {
        return input;
    }

    public WrapperParams setInput(List<String> input) {
        this.input = input;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public WrapperParams setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
