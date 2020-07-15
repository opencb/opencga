package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;

public class OpenCgaMongoConverter<T> extends GenericDocumentComplexConverter<T> {

    public OpenCgaMongoConverter(Class<T> clazz) {
        super(clazz);
    }

    public OpenCgaMongoConverter(Class<T> clazz, ObjectMapper objectMapper) {
        super(clazz, objectMapper);
    }

    /**
     * This method returns the long value of the key given.
     * @param document Document with the field
     * @param key Field name to be converted
     * @return Long converted value
     */
    public static Long getLongValue(Document document, String key) {
        try {
            return document.getLong(key);
        } catch (ClassCastException e) {
            return document.getInteger(key).longValue();
        }
    }

}
