package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.models.ClinicalAnalysis;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisConverter extends GenericDocumentComplexConverter<ClinicalAnalysis> {

    public ClinicalAnalysisConverter() {
        super(ClinicalAnalysis.class);
    }

    @Override
    public Document convertToStorageType(ClinicalAnalysis object) {
        Document document = super.convertToStorageType(object);
        document.put("id", document.getInteger("id").longValue());

        long familyId = object.getFamily() != null ? (object.getFamily().getId() == 0 ? -1L : object.getFamily().getId()) : -1L;
        document.put("family", new Document("id", familyId));

        long probandId = object.getProband() != null ? (object.getProband().getId() == 0 ? -1L : object.getProband().getId()) : -1L;
        document.put("proband", new Document("id", probandId));

        long sampleId = object.getSample() != null ? (object.getSample().getId() == 0 ? -1L : object.getSample().getId()) : -1L;
        document.put("sample", new Document("id", sampleId));

        return document;
    }
}
