package org.opencb.opencga.server.rest.json.beans;

import java.util.List;

public class Category {

    private String name;
    private String path;
    private List<Endpoint> endpoints;

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

    @Override
    public String toString() {
        return "Category{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", endpoints=" + endpoints +
                "}\n";
    }
}
