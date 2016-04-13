package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.models.Filter;

import java.io.IOException;
/**
 * Created by pfurio on 13/04/16.
 */
public class FilterConverter extends GenericConverter<Filter, Document> {

    private ObjectWriter fileWriter;

    public FilterConverter() {
        objectReader = objectMapper.reader(Filter.class);
        fileWriter = objectMapper.writerFor(Filter.class);
    }

    @Override
    public Filter convertToDataModelType(Document object) {
        Filter filter = null;
        try {
            Document filters = (Document) ((Document) object.get("configs")).get("opencga-filters");
            filter = objectReader.readValue(objectWriter.writeValueAsString(filters));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return filter;
    }

    @Override
    public Document convertToStorageType(Filter object) {
        Document document = null;
        try {
            document = Document.parse(fileWriter.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
