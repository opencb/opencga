package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.Study;

import java.io.IOException;

/**
 * Created by pfurio on 18/01/16.
 */
public class StudyConverter extends GenericConverter<Study, Document> {

    private ObjectWriter studyWriter;

    public StudyConverter() {
        objectReader = objectMapper.reader(Study.class);
        studyWriter = objectMapper.writerFor(Study.class);
    }

    @Override
    public Study convertToDataModelType(Document object) {
        Study study = null;
        try {
            study = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return study;
    }

    @Override
    public Document convertToStorageType(Study object) {
        Document document = null;
        try {
            document = Document.parse(studyWriter.writeValueAsString(object));
            document.put("id", document.getInteger("id").longValue());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
