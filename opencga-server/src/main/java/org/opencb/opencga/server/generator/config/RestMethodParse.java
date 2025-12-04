package org.opencb.opencga.server.generator.config;

import org.opencb.commons.annotations.DataField;

public class RestMethodParse {

    @DataField(id = "rest", description = "Exact REST endpoint. Example: 'tools/custom/run'")
    private String rest;

    @DataField(id = "methodName", description = "Method name associated to the REST endpoint in kebab case. Example: 'run-custom-docker'")
    private String methodName;

    public RestMethodParse() {
    }

    public RestMethodParse(String rest, String methodName) {
        this.rest = rest;
        this.methodName = methodName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RestMethodParse{");
        sb.append("rest='").append(rest).append('\'');
        sb.append(", methodName='").append(methodName).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getRest() {
        return rest;
    }

    public RestMethodParse setRest(String rest) {
        this.rest = rest;
        return this;
    }

    public String getMethodName() {
        return methodName;
    }

    public RestMethodParse setMethodName(String methodName) {
        this.methodName = methodName;
        return this;
    }
}
