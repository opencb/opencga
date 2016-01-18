package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.databind.*;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pfurio on 18/01/16.
 */
public abstract class GenericConverter<M, D> implements ComplexTypeConverter<M, D> {

    protected ObjectMapper objectMapper;
    protected ObjectReader objectReader;
    protected ObjectWriter objectWriter;

    protected Logger logger;

    public GenericConverter() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        logger = LoggerFactory.getLogger(this.getClass());
    }

}
