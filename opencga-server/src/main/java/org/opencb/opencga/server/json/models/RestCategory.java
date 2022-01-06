package org.opencb.opencga.server.json.models;

import java.util.List;

public class RestCategory {

    private String name;
    private String path;
    private List<RestEndpoint> endpoints;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Category{");
        sb.append("name='").append(name).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", endpoints=").append(endpoints);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public RestCategory setName(String name) {
        this.name = name;
        return this;
    }

    public String getPath() {
        return path;
    }

    public RestCategory setPath(String path) {
        this.path = path;
        return this;
    }

    public List<RestEndpoint> getEndpoints() {
        return endpoints;
    }

    public RestCategory setEndpoints(List<RestEndpoint> restEndpoints) {
        this.endpoints = restEndpoints;
        return this;
    }

}
