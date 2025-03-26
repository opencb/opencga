package org.opencb.opencga.server.generator.openapi.models;
import java.util.HashMap;
import java.util.Map;

public class Schema {

    private FieldDefinition updateParams;
    private String $ref;
    private String type;
    private String format;
    private Schema additionalProperties;
    private Schema items;
    private Map<String, Schema> properties = new HashMap<>();


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Schema getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Schema additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public Map<String, Schema> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Schema> properties) {
        this.properties = properties;
    }

    public void addProperty(String name, Schema schema) {
        this.properties.put(name, schema);
    }
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

    public String getFormat() {
        return format;
    }

    public Schema setFormat(String format) {
        this.format = format;
        return this;
    }

    public Schema getItems() {
        return items;
    }

    public Schema setItems(Schema items) {
        this.items = items;
        return this;
    }
}
