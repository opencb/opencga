package org.opencb.opencga.core.models.wrapper;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiQcParams {

    protected List<String> input;
    protected ObjectMap params;

    public MultiQcParams() {
        this.input = new ArrayList<>();
        this.params = new ObjectMap();
    }

    public MultiQcParams(List<String> input, ObjectMap params) {
        this.input = input;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MultiQcParams{");
        sb.append("input=").append(input);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getInput() {
        return input;
    }

    public MultiQcParams setInput(List<String> input) {
        this.input = input;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public MultiQcParams setParams(ObjectMap params) {
        this.params = params;
        return this;
    }
}
