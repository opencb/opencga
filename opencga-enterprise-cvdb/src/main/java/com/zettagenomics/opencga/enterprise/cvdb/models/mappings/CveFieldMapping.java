package com.zettagenomics.opencga.enterprise.cvdb.models.mappings;

import java.util.HashMap;
import java.util.Map;

import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;

public class CveFieldMapping extends FieldMapping {

    public CveFieldMapping() {
        modelToSchema = new HashMap();
        modelToSchema.put(CVE_PHENOTYPE_NAME_NAME, "phenotypeNames");
        modelToSchema.put(CVE_GENE_NAME_NAME, "geneName");
        modelToSchema.put(CVE_SO_TERM_ACCESSION_NAME, "consequenceTypeIds");
        modelToSchema.put(CVE_XREF_ID_NAME, "xrefIds");
        modelToSchema.put(CVE_PANEL_ID_NAME, "panelId");
        modelToSchema.put(CVE_MOI_NAME, "mois");
        modelToSchema.put(CVE_PENETRANCE_NAME, "penetrance");
        modelToSchema.put(CVE_ACGM_NAME, "acmgs");
        modelToSchema.put(CVE_TIER_NAME, "tier");
        modelToSchema.put(CVE_CLINICAL_SIGNIFICANCE_NAME, "clinicalSignificance");
        modelToSchema.put(CVE_DRUG_RESPONSE_NAME, "drugResponse");
        modelToSchema.put(CVE_TRAIT_ASSOCIATION_NAME, "traitAssociation");
        modelToSchema.put(CVE_FUNCTIONAL_EFFECT_NAME, "functionalEffect");
        modelToSchema.put(CVE_TUMORIGENESIS_NAME, "tumorigenesis");
        modelToSchema.put(CVE_OTHER_CLASSIFICATION_NAME, "otherClassifications");
        modelToSchema.put(CVE_ROLE_IN_CANCER_NAME, "rolesInCancer");

        schemaToModel = new HashMap();
        for (Map.Entry<String, String> entry : modelToSchema.entrySet()) {
            schemaToModel.put(entry.getValue(), entry.getKey());
        }
    }
}
