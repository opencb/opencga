package org.opencb.opencga.core.config.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.opencb.opencga.core.common.JacksonUtils;

import static org.junit.Assert.*;

public class IndexFieldConfigurationTest {

    @Test
    public void testDeserialize() throws JsonProcessingException {
        assertEquals(IndexFieldConfiguration.Type.RANGE_LT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE\"", IndexFieldConfiguration.Type.class));
        assertEquals(IndexFieldConfiguration.Type.RANGE_LT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE_LT\"", IndexFieldConfiguration.Type.class));
        assertEquals(IndexFieldConfiguration.Type.RANGE_LT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE_GE\"", IndexFieldConfiguration.Type.class));

        assertEquals(IndexFieldConfiguration.Type.RANGE_GT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE_GT\"", IndexFieldConfiguration.Type.class));
        assertEquals(IndexFieldConfiguration.Type.RANGE_GT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE_LE\"", IndexFieldConfiguration.Type.class));
        assertEquals(IndexFieldConfiguration.Type.RANGE_GT, JacksonUtils.getDefaultObjectMapper().readValue("\"rangeLe\"", IndexFieldConfiguration.Type.class));

        assertEquals(IndexFieldConfiguration.Type.CATEGORICAL, JacksonUtils.getDefaultObjectMapper().readValue("\"Categorical\"", IndexFieldConfiguration.Type.class));
        assertEquals(IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE, JacksonUtils.getDefaultObjectMapper().readValue("\"CategoricalMultiValue\"", IndexFieldConfiguration.Type.class));
    }

}