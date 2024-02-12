package com.zettagenomics.opencga.enterprise.cvdb.models.mappings;

import java.util.HashMap;
import java.util.Map;

import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;

public class CiFieldMapping extends FieldMapping {

    public CiFieldMapping() {
        modelToSchema = new HashMap();
        modelToSchema.put(CI_ID_NAME, "id");
        modelToSchema.put(CI_PRIMARY_NAME, "primary");
        modelToSchema.put(CI_PANEL_ID_NAME, "panelIds");
        modelToSchema.put(CI_ANALYIST_ID_NAME, "analystId");
        modelToSchema.put(CI_ANALYIST_NAME_NAME, "analystName");
        modelToSchema.put(CI_ANALYIST_EMAIL_NAME, "analystEmail");
        modelToSchema.put(CI_ANALYIST_ASSIGNED_BY_NAME, "analystAssignedBy");
        modelToSchema.put(CI_ANALYIST_DATE_NAME, "analystDate");
        modelToSchema.put(CI_METHOD_NAME_NAME, "methodName");
        modelToSchema.put(CI_METHOD_VERSION_NAME, "methodVersion");
        modelToSchema.put(CI_METHOD_COMMIT_NAME, "methodCommit");
        modelToSchema.put(CI_LOCKED_NAME, "locked");
        modelToSchema.put(CI_STATUS_ID_NAME, "statusId");
        modelToSchema.put(CI_STATUS_NAME_NAME, "statusName");
        modelToSchema.put(CI_STATUS_DATE_NAME, "statusDate");
        modelToSchema.put(CI_CREATION_DATE_NAME, "creationDate");
        modelToSchema.put(CI_MODIFICATION_DATE_NAME, "modificationDate");
        modelToSchema.put(CI_VERSION_NAME, "version");

        schemaToModel = new HashMap();
        for (Map.Entry<String, String> entry : modelToSchema.entrySet()) {
            schemaToModel.put(entry.getValue(), entry.getKey());
        }
    }
}
