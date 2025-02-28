package org.opencb.opencga.server.generator.openapi.models;

public class FieldDefinition {

    private String type;
    private String $ref;
    private String ref;


    public FieldDefinition() {
    }

    public FieldDefinition(String ref) {
        this.$ref = ref;
    }

    public String getType() {
        return type;
    }

    public FieldDefinition setType(String type) {
        this.type = type;
        return this;
    }

    public String get$ref() {
        return $ref;
    }

    public FieldDefinition set$ref(String $ref) {
        this.$ref = $ref;
        return this;
    }


    public String getRef() {
        return ref;
    }

    public FieldDefinition setRef(String ref) {
        this.ref = ref;
        return this;
    }

}


