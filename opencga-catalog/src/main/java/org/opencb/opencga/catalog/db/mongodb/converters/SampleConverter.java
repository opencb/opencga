package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.Sample;

import java.io.IOException;

/**
 * Created by pfurio on 19/01/16.
 */
public class SampleConverter extends GenericConverter<Sample, Document> {

    private ObjectWriter sampleWriter;

    public SampleConverter() {
        objectReader = objectMapper.reader(Sample.class);
        sampleWriter = objectMapper.writerFor(Sample.class);
    }

    @Override
    public Sample convertToDataModelType(Document object) {
        Sample sample = null;
        try {
            sample = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sample;
    }

    @Override
    public Document convertToStorageType(Sample object) {
        Document document = null;
        try {
            document = Document.parse(sampleWriter.writeValueAsString(object));
            document.put("id", document.getInteger("id").longValue());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
