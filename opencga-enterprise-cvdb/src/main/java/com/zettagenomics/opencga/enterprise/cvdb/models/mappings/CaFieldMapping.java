package com.zettagenomics.opencga.enterprise.cvdb.models.mappings;

import org.opencb.biodata.models.variant.annotation.exceptions.SOTermNotAvailableException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;

public class CaFieldMapping extends FieldMapping {

    public CaFieldMapping() {
        modelToSchema = new HashMap();
        modelToSchema.put(CA_TYPE_NAME, "type");
        modelToSchema.put(CA_DISORDER_ID_NAME, "disorderId");
        modelToSchema.put(CA_FILENAME_NAME, "fileNames");
        modelToSchema.put(CA_PROBAND_ID_NAME, "probandId");
        modelToSchema.put(CA_FAMILY_ID_NAME, "familyId");
        modelToSchema.put(CA_FAMILY_PHENOTYPE_NAME_NAME, "familyPhenotypeNames");
        modelToSchema.put(CA_FAMILY_MEMBER_ID_NAME, "familyMemberIds");
        modelToSchema.put(CA_STATUS_NAME, "status");
        modelToSchema.put(CA_LOCKED_NAME, "locked");

        schemaToModel = new HashMap();
        for (Map.Entry<String, String> entry : modelToSchema.entrySet()) {
            schemaToModel.put(entry.getValue(), entry.getKey());
        }
    }
}
