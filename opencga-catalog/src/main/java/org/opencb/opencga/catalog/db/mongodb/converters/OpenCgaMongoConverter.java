package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.common.JacksonUtils;

public class OpenCgaMongoConverter<T> extends GenericDocumentComplexConverter<T> {

    public OpenCgaMongoConverter(Class<T> clazz) {
        super(clazz, JacksonUtils.getDefaultObjectMapper());
    }

    public OpenCgaMongoConverter(Class<T> clazz, ObjectMapper objectMapper) {
        super(clazz, objectMapper);
    }

}
