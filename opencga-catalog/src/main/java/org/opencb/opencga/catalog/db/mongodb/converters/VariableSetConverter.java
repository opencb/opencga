package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.VariableSet;

import java.io.IOException;

/**
 * Created by pfurio on 04/04/16.
 */
public class VariableSetConverter extends GenericConverter<VariableSet, Document>  {
    private ObjectWriter variableSetWriter;

    public VariableSetConverter() {
        objectReader = objectMapper.reader(VariableSet.class);
        variableSetWriter = objectMapper.writerFor(VariableSet.class);
    }

    @Override
    public VariableSet convertToDataModelType(Document object) {
        VariableSet variableSet = null;
        try {
            variableSet = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return variableSet;
    }

    @Override
    public Document convertToStorageType(VariableSet object) {
        Document document = null;
        try {
            document = Document.parse(variableSetWriter.writeValueAsString(object));
            document.put("id", document.getInteger("id").longValue());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
