package org.opencb.opencga.server.generator.openapi.models;

public class Property {

    private String type;
    private String ref;
    private Property items;

    public Property() {
    }

    public Property(String ref) {
        this.ref = ref;
    }

    // Getters y Setters
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public Property getItems() {
        return items;
    }

    public void setItems(Property items) {
        this.items = items;
    }
}


