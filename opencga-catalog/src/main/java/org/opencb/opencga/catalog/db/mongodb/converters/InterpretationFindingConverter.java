package org.opencb.opencga.catalog.db.mongodb.converters;


import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;

public class InterpretationFindingConverter extends OpenCgaMongoConverter<ClinicalVariant> {

    public InterpretationFindingConverter() {
        super(ClinicalVariant.class);
    }
}
