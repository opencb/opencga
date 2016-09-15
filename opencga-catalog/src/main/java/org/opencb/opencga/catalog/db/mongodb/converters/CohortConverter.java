package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.Cohort;

import java.io.IOException;
/**
 * Created by pfurio on 3/22/16.
 */
public class CohortConverter extends GenericConverter<Cohort, Document> {

    private ObjectWriter CohortWriter;

    public CohortConverter() {
        objectReader = objectMapper.reader(Cohort.class);
        CohortWriter = objectMapper.writerFor(Cohort.class);
    }

    @Override
    public Cohort convertToDataModelType(Document object) {
        Cohort cohort = null;
        try {
            cohort = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cohort;
    }

    @Override
    public Document convertToStorageType(Cohort object) {
        Document document = null;
        try {
            document = Document.parse(CohortWriter.writeValueAsString(object));
            document.put("id", document.getInteger("id").longValue());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }

}
