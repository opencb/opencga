package org.opencb.opencga.server.generator.openapi.models;

public class Schema {

    private FieldDefinition updateParams;
    private String $ref;

    public Schema() {

    }

    public String get$ref() {
        return $ref;
    }

    public Schema set$ref(String $ref) {
        this.$ref = $ref;
        return this;
    }

    public FieldDefinition getUpdateParams() {
        return updateParams;
    }

    public Schema setUpdateParams(FieldDefinition updateParams) {
        this.updateParams = updateParams;
        return this;
    }
}
