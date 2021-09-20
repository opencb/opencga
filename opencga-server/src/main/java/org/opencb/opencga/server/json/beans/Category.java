package org.opencb.opencga.server.json.beans;

import java.util.List;

public class Category {

    private String name;
    private String path;
    private List<Endpoint> endpoints;

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

    public Category setName(String name) {
        this.name = name;
        return this;
    }

    public String getPath() {
        return path;
    }

    public Category setPath(String path) {
        this.path = path;
        return this;
    }

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public Category setEndpoints(List<Endpoint> endpoints) {
        this.endpoints = endpoints;
        return this;
    }

}
