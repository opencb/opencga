package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.Document;
import org.opencb.opencga.catalog.models.Job;

import java.io.IOException;

/**
 * Created by pfurio on 19/01/16.
 */
public class JobConverter extends GenericConverter<Job, Document> {

    public JobConverter() {
        objectReader = objectMapper.reader(Job.class);
    }

    @Override
    public Job convertToDataModelType(Document object) {
        Job job = null;
        try {
            job = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    @Override
    public Document convertToStorageType(Job object) {
        Document document = null;
        try {
            document = Document.parse(objectWriter.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
