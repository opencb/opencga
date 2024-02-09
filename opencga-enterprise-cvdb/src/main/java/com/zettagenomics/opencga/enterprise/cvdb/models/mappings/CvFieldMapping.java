package com.zettagenomics.opencga.enterprise.cvdb.models.mappings;

import java.util.HashMap;
import java.util.Map;

import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;

public class CvFieldMapping extends FieldMapping {

    public CvFieldMapping() {
        modelToSchema = new HashMap();
        modelToSchema.put(CV_ID_NAME, "variantId");
        modelToSchema.put(CV_PRIMARY_NAME, "primary");
        modelToSchema.put(CV_DISCUSSION_AUTHOR_NAME, "discussionAuthor");
        modelToSchema.put(CV_DISCUSSION_DATE_NAME, "discussionDate");
        modelToSchema.put(CV_CONFIDENCE_VALUE_NAME, "confidenceValue");
        modelToSchema.put(CV_CONFIDENCE_AUTHOR_NAME, "confidenceAuthor");
        modelToSchema.put(CV_CONFIDENCE_DATE_NAME, "confidenceDate");
        modelToSchema.put(CV_TAG_NAME, "tags");
        modelToSchema.put(CV_STATUS_NAME, "status");
        modelToSchema.put(CV_ANNOT_BIOTYPE_NAME, "biotypes");
        modelToSchema.put(CV_ANNOT_CONSEQUENCE_TYPE_NAME, "soAcc");
        modelToSchema.put(CV_GENE_NAME, "genes");
        modelToSchema.put(CV_ANNOT_XREF_NAME, "xrefs");
        modelToSchema.put(CV_TYPE_NAME, "type");

        schemaToModel = new HashMap();
        for (Map.Entry<String, String> entry : modelToSchema.entrySet()) {
            schemaToModel.put(entry.getValue(), entry.getKey());
        }
    }
}
