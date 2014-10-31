package org.opencb.opencga.analysis.beans;

public class Option {
    private String name, description;
    private boolean required;

    public Option() {

    }

    public Option(String name, String description, boolean required) {
        this.name = name;
        this.description = description;
        this.required = required;
    }

    @Override
    public String toString() {
        return "Option{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", required=" + required +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
