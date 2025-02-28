package org.opencb.opencga.server.generator.openapi.models;

public class ListDefinition extends FieldDefinition {

    private FieldDefinition items;

    public ListDefinition() {
        super();
    }

    public FieldDefinition getItems() {
        return items;
    }

    public ListDefinition setItems(FieldDefinition items) {
        this.items = items;
        return this;
    }
}
