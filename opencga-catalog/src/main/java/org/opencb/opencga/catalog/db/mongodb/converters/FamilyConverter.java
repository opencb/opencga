package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.models.Family;

/**
 * Created by pfurio on 03/05/17.
 */
public class FamilyConverter extends GenericDocumentComplexConverter<Family> {

    public FamilyConverter() {
        super(Family.class);
    }

    @Override
    public Document convertToStorageType(Family object) {
        Document document = super.convertToStorageType(object);
        document.put("id", document.getInteger("id").longValue());
        return document;
    }

}
