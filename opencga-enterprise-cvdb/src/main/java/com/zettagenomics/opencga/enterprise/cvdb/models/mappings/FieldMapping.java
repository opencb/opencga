package com.zettagenomics.opencga.enterprise.cvdb.models.mappings;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;

import java.util.Map;

public class FieldMapping {
    protected Map<String, String> modelToSchema;
    protected Map<String, String> schemaToModel;

    public String toModelField(String input) throws CvdbException {
        if (!schemaToModel.containsKey(input)) {
            throw new CvdbException("Invalid file '" + input + "' in Solr schema");
        }
        return schemaToModel.get(input);
    }

    public String toSchemaField(String input) throws CvdbException {
        if (!modelToSchema.containsKey(input)) {
            throw new CvdbException("Invalid file '" + input + "' in OpenCGA data model");
        }
        return modelToSchema.get(input);
    }
}
