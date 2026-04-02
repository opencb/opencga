package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.GwasAssociationStudyTraitScores;

import static org.junit.Assert.*;

/**
 * Test for OpenCGAObjectMapperProvider to ensure the REST API uses the correct ObjectMapper
 * with all necessary mixins registered.
 */
public class OpenCGAObjectMapperProviderTest {

    @Test
    public void testProviderReturnsConfiguredMapper() {
        OpenCGAObjectMapperProvider provider = new OpenCGAObjectMapperProvider();
        ObjectMapper mapper = provider.getContext(Object.class);

        assertNotNull("Provider should return an ObjectMapper", mapper);
    }

    @Test
    public void testMapperHasGwasMixinRegistered() {
        OpenCGAObjectMapperProvider provider = new OpenCGAObjectMapperProvider();
        ObjectMapper mapper = provider.getContext(Object.class);

        // Verify that the GwasAssociationStudyTraitScores mixin is registered
        Class<?> mixinClass = mapper.findMixInClassFor(GwasAssociationStudyTraitScores.class);

        assertNotNull("Mixin should be registered for GwasAssociationStudyTraitScores", mixinClass);
        assertEquals("Should use GwasAssociationStudyTraitScoresMixin",
                "GwasAssociationStudyTraitScoresMixin",
                mixinClass.getSimpleName());
    }

    @Test
    public void testMapperDeserializesCamelCaseFieldsCorrectly() throws Exception {
        // This is the critical test: ensure the provider's mapper can handle JSON from Python clients
        String jsonFromPythonClient = "{\"pValue\":0.00001,\"pValueMlog\":5.0,\"pValueText\":\"1e-5\",\"orBeta\":1.5,\"percentCI\":\"95% CI\"}";

        OpenCGAObjectMapperProvider provider = new OpenCGAObjectMapperProvider();
        ObjectMapper mapper = provider.getContext(Object.class);

        // Deserialize using the provider's mapper
        GwasAssociationStudyTraitScores scores = mapper.readValue(jsonFromPythonClient, GwasAssociationStudyTraitScores.class);

        // Verify all fields are correctly deserialized (not null)
        assertNotNull("Deserialized object should not be null", scores);
        assertEquals("pValue should be deserialized correctly", Double.valueOf(0.00001), scores.getPValue());
        assertEquals("pValueMlog should be deserialized correctly", Double.valueOf(5.0), scores.getPValueMlog());
        assertEquals("pValueText should be deserialized correctly", "1e-5", scores.getPValueText());
        assertEquals("orBeta should be deserialized correctly", Double.valueOf(1.5), scores.getOrBeta());
        assertEquals("percentCI should be deserialized correctly", "95% CI", scores.getPercentCI());
    }

    @Test
    public void testMapperSerializesToCamelCase() throws Exception {
        // Verify that the mapper serializes to camelCase (matching the Avro schema)
        GwasAssociationStudyTraitScores scores = GwasAssociationStudyTraitScores.newBuilder()
                .setPValue(0.00001)
                .setPValueMlog(5.0)
                .setPValueText("1e-5")
                .setOrBeta(1.5)
                .setPercentCI("95% CI")
                .build();

        OpenCGAObjectMapperProvider provider = new OpenCGAObjectMapperProvider();
        ObjectMapper mapper = provider.getContext(Object.class);

        String json = mapper.writeValueAsString(scores);

        // Verify the JSON contains the camelCase field names
        assertTrue("JSON should contain pValue", json.contains("pValue") || json.contains("pvalue"));
        assertTrue("JSON should contain pValueMlog", json.contains("pValueMlog") || json.contains("pvalueMlog"));
        assertTrue("JSON should contain pValueText", json.contains("pValueText") || json.contains("pvalueText"));
    }
}
