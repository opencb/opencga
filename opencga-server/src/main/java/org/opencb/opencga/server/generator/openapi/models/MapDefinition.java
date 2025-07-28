package org.opencb.opencga.server.generator.openapi.models;

public class MapDefinition extends FieldDefinition {

    public MapDefinition() {
        super();
        setType("object");
    }

    public MapDefinition(Schema content) {
        super();
        setType("object");
        setAdditionalProperties(content);
    }

    public void setAdditionalProperties$Ref(String additionalProperties$Ref) {
        setAdditionalProperties(new Schema().set$ref(additionalProperties$Ref));
    }

    public void setAdditionalPropertiesType(String additionalPropertiesType) {
        setAdditionalProperties(new Schema().setType(additionalPropertiesType));
    }


}
