package org.opencb.opencga.server.generator.openapi.models;

public class MapDefinition extends FieldDefinition {

    private FieldDefinition key;
    private FieldDefinition value;


    public MapDefinition() {
        super();
    }

    public FieldDefinition getKey() {
        return key;
    }

    public MapDefinition setKey(FieldDefinition key) {
        this.key = key;
        return this;
    }

    public FieldDefinition getValue() {
        return value;
    }

    public MapDefinition setValue(FieldDefinition value) {
        this.value = value;
        return this;
    }
}
