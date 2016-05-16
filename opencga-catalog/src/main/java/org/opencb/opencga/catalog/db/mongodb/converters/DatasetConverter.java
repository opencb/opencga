package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.Dataset;

import java.io.IOException;
/**
 * Created by pfurio on 04/05/16.
 */
public class DatasetConverter extends GenericConverter<Dataset, Document> {

    private ObjectWriter datasetWriter;

    public DatasetConverter() {
        objectReader = objectMapper.reader(Dataset.class);
        datasetWriter = objectMapper.writerFor(Dataset.class);
    }

    @Override
    public Dataset convertToDataModelType(Document object) {
        Dataset dataset = null;
        try {
            dataset = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataset;
    }

    @Override
    public Document convertToStorageType(Dataset object) {
        Document document = null;
        try {
            document = Document.parse(datasetWriter.writeValueAsString(object));
            document.put("id", document.getInteger("id").longValue());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }

}
