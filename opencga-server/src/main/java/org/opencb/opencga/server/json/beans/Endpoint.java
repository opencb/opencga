package org.opencb.opencga.server.json.beans;

import java.util.List;

public class Endpoint {

    private String path;
    private String method;
    private String response;
    private String responseClass;
    private String notes;
    private String description;
    private List<Parameter> parameters;

    public String getPath() {
        return path;
    }

    public Endpoint setPath(String path) {
        this.path = path;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public Endpoint setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getResponse() {
        return response;
    }

    public Endpoint setResponse(String response) {
        this.response = response;
        return this;
    }

    public String getResponseClass() {
        return responseClass;
    }

    public Endpoint setResponseClass(String responseClass) {
        this.responseClass = responseClass;
        return this;
    }

    public String getNotes() {
        return notes;
    }

    public Endpoint setNotes(String notes) {
        this.notes = notes;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Endpoint setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public Endpoint setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
        return this;
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "path='" + path + '\'' +
                ", method='" + method + '\'' +
                ", response='" + response + '\'' +
                ", responseClass='" + responseClass + '\'' +
                ", notes='" + notes + '\'' +
                ", description='" + description + '\'' +
                ", parameters=" + parameters +
                "}\n";
    }
}
