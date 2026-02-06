package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.core.common.JacksonUtils;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * Provides the configured ObjectMapper with all necessary mixins for the OpenCGA REST API.
 *
 * This provider ensures that Jersey uses the properly configured ObjectMapper from JacksonUtils,
 * which includes critical mixins for Avro models such as GwasAssociationStudyTraitScores.
 *
 * Without this provider, Jersey would use a default ObjectMapper that doesn't recognize
 * camelCase field names (pValue, pValueMlog, pValueText) from the biodata Avro schemas,
 * causing deserialization failures when Python clients send JSON data.
 *
 * @see org.opencb.opencga.core.common.JacksonUtils
 * @see org.opencb.opencga.core.models.common.mixins.GwasAssociationStudyTraitScoresMixin
 */
@Provider
public class OpenCGAObjectMapperProvider implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    public OpenCGAObjectMapperProvider() {
        // Use the default non-null mapper which includes all variant mixins
        // (GwasAssociationStudyTraitScores, VariantAnnotation, ConsequenceType, etc.)
        mapper = JacksonUtils.getDefaultNonNullObjectMapper();
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}
