package org.opencb.opencga.core.config.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class FieldConfigurationTest {

    @Test
    public void testDeserialize() throws JsonProcessingException {
        assertEquals(FieldConfiguration.Type.RANGE_LT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE\"", FieldConfiguration.Type.class));
        assertEquals(FieldConfiguration.Type.RANGE_LT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE_LT\"", FieldConfiguration.Type.class));
        assertEquals(FieldConfiguration.Type.RANGE_LT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE_GE\"", FieldConfiguration.Type.class));

        assertEquals(FieldConfiguration.Type.RANGE_GT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE_GT\"", FieldConfiguration.Type.class));
        assertEquals(FieldConfiguration.Type.RANGE_GT, JacksonUtils.getDefaultObjectMapper().readValue("\"RANGE_LE\"", FieldConfiguration.Type.class));
        assertEquals(FieldConfiguration.Type.RANGE_GT, JacksonUtils.getDefaultObjectMapper().readValue("\"rangeLe\"", FieldConfiguration.Type.class));

        assertEquals(FieldConfiguration.Type.CATEGORICAL, JacksonUtils.getDefaultObjectMapper().readValue("\"Categorical\"", FieldConfiguration.Type.class));
        assertEquals(FieldConfiguration.Type.CATEGORICAL_MULTI_VALUE, JacksonUtils.getDefaultObjectMapper().readValue("\"CategoricalMultiValue\"", FieldConfiguration.Type.class));
    }

}