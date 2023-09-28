package org.opencb.opencga.app.cli.conf;

import java.util.Arrays;

public class Category {

    private String name;
    private String description;
    private String[] options;

    public Category() {
    }

    public Category(String name, String description, String[] options) {
        this.name = name;
        this.description = description;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Category{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", options=").append(Arrays.toString(options));
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

    public String getDescription() {
        return description;
    }

    public Category setDescription(String description) {
        this.description = description;
        return this;
    }

    public String[] getOptions() {
        return options;
    }

    public Category setOptions(String[] options) {
        this.options = options;
        return this;
    }
}
