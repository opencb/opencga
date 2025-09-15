package org.opencb.opencga.server.generator.openapi.models;

import java.util.List;

public class FieldDefinition extends Schema {

    private String ref;
    private String description;
    // List of possible values. If nullable, this list should include null
    private List<String> enumValues = null; // Enum values


    public FieldDefinition() {
    }

    public FieldDefinition(String ref) {
        super();
        set$ref(ref);
    }

    public FieldDefinition setType(String type) {
        super.setType(type);
        return this;
    }

    public FieldDefinition set$ref(String $ref) {
        super.set$ref($ref);
        return this;
    }

    public String getRef() {
        return ref;
    }

    public FieldDefinition setRef(String ref) {
        this.ref = ref;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FieldDefinition setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<String> getEnum() {
        return enumValues;
    }

    public FieldDefinition setEnum(List<String> enumValues) {
        this.enumValues = enumValues;
        return this;
    }
}


