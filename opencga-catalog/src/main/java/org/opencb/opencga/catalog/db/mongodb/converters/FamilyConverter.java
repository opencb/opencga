package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.models.Family;
import org.opencb.opencga.catalog.models.Individual;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

        long motherId = object.getMother() != null ? (object.getMother().getId() == 0 ? -1L : object.getMother().getId()) : -1L;
        document.put("mother", new Document("id", motherId));

        long fatherId = object.getFather() != null ? (object.getFather().getId() == 0 ? -1L : object.getFather().getId()) : -1L;
        document.put("father", new Document("id", fatherId));

        if (object.getChildren() != null) {
            List<Document> children = new ArrayList();
            for (Individual individual : object.getChildren()) {
                long individualId = individual != null ? (individual.getId() == 0 ? -1L : individual.getId()) : -1L;
                if (individualId > 0) {
                    children.add(new Document("id", individualId));
                }
            }
            if (children.size() > 0) {
                document.put("children", children);
            }
        } else {
            document.put("children", Collections.emptyList());
        }

        return document;
    }

}
