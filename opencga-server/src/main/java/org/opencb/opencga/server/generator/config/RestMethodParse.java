package org.opencb.opencga.server.generator.config;

public class RestMethodParse {

    private String rest;
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
