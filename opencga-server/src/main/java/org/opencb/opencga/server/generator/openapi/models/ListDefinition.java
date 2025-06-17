package org.opencb.opencga.server.generator.openapi.models;

public class ListDefinition extends FieldDefinition {

    public ListDefinition() {
        super();
        setType("array");
    }

    public ListDefinition(FieldDefinition items) {
        super();
        setType("array");
        setItems(items);
    }


    public ListDefinition setItems(FieldDefinition items) {
        super.setItems(items);
        return this;
    }
}
