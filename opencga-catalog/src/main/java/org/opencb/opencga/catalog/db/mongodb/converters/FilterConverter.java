package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.QueryFilter;

import java.io.IOException;
/**
 * Created by pfurio on 13/04/16.
 */
public class FilterConverter extends GenericConverter<QueryFilter, Document> {

    private ObjectWriter fileWriter;

    public FilterConverter() {
        objectReader = objectMapper.reader(QueryFilter.class);
        fileWriter = objectMapper.writerFor(QueryFilter.class);
    }

    @Override
    public QueryFilter convertToDataModelType(Document object) {
        QueryFilter queryFilter = null;
        try {
            Document filters = (Document) ((Document) object.get("configs")).get("opencga-filters");
            queryFilter = objectReader.readValue(objectWriter.writeValueAsString(filters));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queryFilter;
    }

    @Override
    public Document convertToStorageType(QueryFilter object) {
        Document document = null;
        try {
            document = Document.parse(fileWriter.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
