package org.opencb.opencga.server.generator.openapi.models;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Tag {
    private String name;
    private String description;
    @JsonIgnore
    private int count;

    public Tag() {
    }

    public Tag(String name, String description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public String toString() {
        return "Tag{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", count=" + count +
                '}';
    }

    public String getName() {
        return name;
    }

    public Tag setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Tag setDescription(String description) {
        this.description = description;
        return this;
    }

    public int getCount() {
        return count;
    }

    public Tag setCount(int count) {
        this.count = count;
        return this;
    }
}
