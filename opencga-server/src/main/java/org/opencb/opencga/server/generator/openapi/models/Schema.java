package org.opencb.opencga.server.generator.openapi.models;

import com.amazonaws.services.dynamodbv2.xspec.S;

public class Schema {

    public Schema() {

    }

    public Schema(String $ref) {
        this.$ref = $ref;
    }

    public String get$ref() {
        return $ref;
    }

    public Schema set$ref(String $ref) {
        this.$ref = $ref;
        return this;
    }

    private String $ref;

}
