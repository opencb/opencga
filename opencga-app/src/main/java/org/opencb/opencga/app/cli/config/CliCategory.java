package org.opencb.opencga.app.cli.config;

import java.util.Arrays;

public class CliCategory {

    private String name;
    private String description;
    private String[] options;

    public CliCategory() {
    }

    public CliCategory(String name, String description, String[] options) {
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

    public CliCategory setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CliCategory setDescription(String description) {
        this.description = description;
        return this;
    }

    public String[] getOptions() {
        return options;
    }

    public CliCategory setOptions(String[] options) {
        this.options = options;
        return this;
    }
}
