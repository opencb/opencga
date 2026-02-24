package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.avro.GwasAssociationStudyTraitScores;
import org.opencb.opencga.core.common.JacksonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
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
@Priority(Priorities.USER)
public class OpenCGAObjectMapperProvider implements ContextResolver<ObjectMapper> {

    private static final Logger logger = LoggerFactory.getLogger(OpenCGAObjectMapperProvider.class);
    private final ObjectMapper mapper;

    public OpenCGAObjectMapperProvider() {
        // Use the default non-null mapper which includes all variant mixins
        // (GwasAssociationStudyTraitScores, VariantAnnotation, ConsequenceType, etc.)
        mapper = JacksonUtils.getDefaultNonNullObjectMapper();

        // Log that the provider was initialized with the mixin
        Class<?> gwasRawMixin = mapper.findMixInClassFor(GwasAssociationStudyTraitScores.class);
        logger.info("OpenCGAObjectMapperProvider initialized");
        logger.info("GwasAssociationStudyTraitScores mixin: {}",
                gwasRawMixin != null ? gwasRawMixin.getSimpleName() : "NOT FOUND");
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        logger.debug("OpenCGAObjectMapperProvider.getContext called for type: {}", type.getSimpleName());
        return mapper;
    }
}
