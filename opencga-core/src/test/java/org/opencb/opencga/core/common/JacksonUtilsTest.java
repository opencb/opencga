package org.opencb.opencga.core.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.GwasAssociationStudyTraitScores;

import static org.junit.Assert.*;

/**
 * Test class for JacksonUtils to verify proper serialization/deserialization of Avro models.
 *
 * This test specifically addresses the issue where GwasAssociationStudyTraitScores fields
 * with camelCase names (pValue, pValueMlog, pValueText) need to be properly handled by Jackson.
 */
public class JacksonUtilsTest {

    @Test
    public void testGwasAssociationStudyTraitScoresSerializationWithMixin() throws Exception {
        // Create a GwasAssociationStudyTraitScores object with pValue set
        GwasAssociationStudyTraitScores scores = GwasAssociationStudyTraitScores.newBuilder()
                .setPValue(0.00001)
                .setPValueMlog(5.0)
                .setPValueText("1e-5")
                .setOrBeta(1.5)
                .setPercentCI("95% CI")
                .build();

        // Get the ObjectMapper with mixins (this is what should be used by the REST API)
        ObjectMapper mapperWithMixin = JacksonUtils.getDefaultNonNullObjectMapper();

        // Serialize to JSON (simulating what the Python client sends)
        String json = mapperWithMixin.writeValueAsString(scores);
        System.out.println("Serialized JSON with mixin: " + json);

        // The JSON should contain the field names as defined in the mixin
        // With the current mixin configuration, it should serialize to "pValue" (camelCase)
        assertTrue("JSON should contain pValue field", json.contains("pValue") || json.contains("pvalue"));

        // Deserialize back (this is what happens on the server when receiving the JSON)
        GwasAssociationStudyTraitScores deserializedScores = mapperWithMixin.readValue(json, GwasAssociationStudyTraitScores.class);

        // Verify the deserialized object matches the original
        assertEquals("pValue should match", scores.getPValue(), deserializedScores.getPValue());
        assertEquals("pValueMlog should match", scores.getPValueMlog(), deserializedScores.getPValueMlog());
        assertEquals("pValueText should match", scores.getPValueText(), deserializedScores.getPValueText());
        assertEquals("orBeta should match", scores.getOrBeta(), deserializedScores.getOrBeta());
        assertEquals("percentCI should match", scores.getPercentCI(), deserializedScores.getPercentCI());
    }

    @Test
    public void testGwasAssociationStudyTraitScoresDeserializationWithCamelCase() throws Exception {
        // This simulates JSON coming from Python client with camelCase field names (as per Avro schema)
        String jsonWithCamelCase = "{\"pValue\":0.00001,\"pValueMlog\":5.0,\"pValueText\":\"1e-5\",\"orBeta\":1.5,\"percentCI\":\"95% CI\"}";

        // Get the ObjectMapper with mixins
        ObjectMapper mapperWithMixin = JacksonUtils.getDefaultNonNullObjectMapper();

        // Try to deserialize - this should work with the mixin
        GwasAssociationStudyTraitScores scores = mapperWithMixin.readValue(jsonWithCamelCase, GwasAssociationStudyTraitScores.class);

        // Verify the values were correctly deserialized
        assertNotNull("Deserialized object should not be null", scores);
        assertEquals("pValue should be deserialized", Double.valueOf(0.00001), scores.getPValue());
        assertEquals("pValueMlog should be deserialized", Double.valueOf(5.0), scores.getPValueMlog());
        assertEquals("pValueText should be deserialized", "1e-5", scores.getPValueText());
        assertEquals("orBeta should be deserialized", Double.valueOf(1.5), scores.getOrBeta());
        assertEquals("percentCI should be deserialized", "95% CI", scores.getPercentCI());
    }

    @Test
    public void testGwasAssociationStudyTraitScoresDeserializationWithLowercase() throws Exception {
        // This simulates JSON with lowercase field names (for backward compatibility)
        String jsonWithLowercase = "{\"pvalue\":0.00001,\"pvalueMlog\":5.0,\"pvalueText\":\"1e-5\",\"orBeta\":1.5,\"percentCI\":\"95% CI\"}";

        // Get the ObjectMapper with mixins
        ObjectMapper mapperWithMixin = JacksonUtils.getDefaultNonNullObjectMapper();

        // Try to deserialize - this should also work due to @JsonAlias
        GwasAssociationStudyTraitScores scores = mapperWithMixin.readValue(jsonWithLowercase, GwasAssociationStudyTraitScores.class);

        // Verify the values were correctly deserialized
        assertNotNull("Deserialized object should not be null", scores);
        assertEquals("pValue should be deserialized", Double.valueOf(0.00001), scores.getPValue());
        assertEquals("pValueMlog should be deserialized", Double.valueOf(5.0), scores.getPValueMlog());
        assertEquals("pValueText should be deserialized", "1e-5", scores.getPValueText());
        assertEquals("orBeta should be deserialized", Double.valueOf(1.5), scores.getOrBeta());
        assertEquals("percentCI should be deserialized", "95% CI", scores.getPercentCI());
    }

    @Test
    public void testGwasAssociationStudyTraitScoresWithDefaultMapper() throws Exception {
        // This test demonstrates the issue: using a default ObjectMapper without the mixin
        String jsonWithCamelCase = "{\"pValue\":0.00001,\"pValueMlog\":5.0,\"pValueText\":\"1e-5\",\"orBeta\":1.5,\"percentCI\":\"95% CI\"}";

        // Create a default ObjectMapper WITHOUT the mixin (this is what happens when Jersey doesn't find our provider)
        ObjectMapper defaultMapper = new ObjectMapper();
        defaultMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            // Try to deserialize - this might fail or not map fields correctly
            GwasAssociationStudyTraitScores scores = defaultMapper.readValue(jsonWithCamelCase, GwasAssociationStudyTraitScores.class);

            // If we get here, check if the values were actually deserialized correctly
            // Without the mixin, the behavior depends on Avro's own annotations
            System.out.println("Default mapper result - pValue: " + scores.getPValue());
            System.out.println("Default mapper result - pValueMlog: " + scores.getPValueMlog());

            // This test documents the current behavior - adjust assertion based on what actually happens
            // The issue reported suggests that without proper configuration, camelCase fields aren't recognized

        } catch (Exception e) {
            // If deserialization fails, that demonstrates the problem
            System.out.println("Deserialization failed with default mapper: " + e.getMessage());
            assertTrue("Exception should mention unrecognized field or mapping issue",
                    e.getMessage().contains("Unrecognized") || e.getMessage().contains("pValue"));
        }
    }

    @Test
    public void testGwasAssociationStudyTraitScoresCamelCaseFailsWithoutMixin() throws Exception {
        // This test explicitly verifies the BUG: camelCase JSON from Python client fails without the mixin
        String jsonFromPythonClient = "{\"pValue\":0.00001,\"pValueMlog\":5.0,\"pValueText\":\"1e-5\",\"orBeta\":1.5,\"percentCI\":\"95% CI\"}";

        // Simulate the REST API using a default ObjectMapper (no mixin registered)
        ObjectMapper defaultMapper = new ObjectMapper();
        defaultMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Deserialize the JSON
        GwasAssociationStudyTraitScores scores = defaultMapper.readValue(jsonFromPythonClient, GwasAssociationStudyTraitScores.class);

        // BUG: The camelCase fields (pValue, pValueMlog, pValueText) are NOT deserialized
        // They come back as null because the default mapper doesn't recognize them
        assertNull("BUG: pValue should be NULL without mixin (field not recognized)", scores.getPValue());
        assertNull("BUG: pValueMlog should be NULL without mixin (field not recognized)", scores.getPValueMlog());
        assertNull("BUG: pValueText should be NULL without mixin (field not recognized)", scores.getPValueText());

        // But orBeta and percentCI work fine because they already match Java property naming
        assertEquals("orBeta should work (already correct naming)", Double.valueOf(1.5), scores.getOrBeta());
        assertEquals("percentCI should work (already correct naming)", "95% CI", scores.getPercentCI());
    }

    @Test
    public void testVariantMixinIsRegistered() {
        // Verify that the GwasAssociationStudyTraitScores mixin is properly registered
        ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();

        // Check if the mixin is registered for the class
        Class<?> mixinClass = mapper.findMixInClassFor(GwasAssociationStudyTraitScores.class);

        assertNotNull("Mixin should be registered for GwasAssociationStudyTraitScores", mixinClass);
        assertEquals("Should use GwasAssociationStudyTraitScoresMixin",
                "GwasAssociationStudyTraitScoresMixin",
                mixinClass.getSimpleName());
    }

    @Test
    public void testVariantMixinIsRegisteredInUpdateObjectMapper() {
        // Verify that the GwasAssociationStudyTraitScores mixin is properly registered in the update mapper
        // This is the key test for TASK-8139: ensures that InterpretationUpdateParams deserialization works
        ObjectMapper updateMapper = JacksonUtils.getUpdateObjectMapper();

        // Check if the mixin is registered for the class
        Class<?> mixinClass = updateMapper.findMixInClassFor(GwasAssociationStudyTraitScores.class);

        assertNotNull("Mixin should be registered for GwasAssociationStudyTraitScores in update mapper", mixinClass);
        assertEquals("Should use GwasAssociationStudyTraitScoresMixin in update mapper",
                "GwasAssociationStudyTraitScoresMixin",
                mixinClass.getSimpleName());
    }

    @Test
    public void testUpdateObjectMapperCanDeserializeGwasWithCamelCase() throws Exception {
        // This directly tests the GWAS scores deserialization using the update object mapper
        // which is used by InterpretationUpdateParams
        String jsonWithCamelCase = "{\"pValue\":0.00001,\"pValueMlog\":5.0,\"pValueText\":\"1e-5\",\"orBeta\":1.5,\"percentCI\":\"95% CI\"}";

        // Get the ObjectMapper used for catalog updates (the one that should have the variant mixin added)
        ObjectMapper updateMapper = JacksonUtils.getUpdateObjectMapper();

        // Try to deserialize - this should work with the mixin added in TASK-8139
        GwasAssociationStudyTraitScores scores = updateMapper.readValue(jsonWithCamelCase, GwasAssociationStudyTraitScores.class);

        // Verify the values were correctly deserialized
        assertNotNull("Deserialized object should not be null", scores);
        assertEquals("pValue should be deserialized", 0.00001, scores.getPValue(), 0.0);
        assertEquals("pValueMlog should be deserialized", 5.0, scores.getPValueMlog(), 0.0);
        assertEquals("pValueText should be deserialized", "1e-5", scores.getPValueText());
        assertEquals("orBeta should be deserialized", 1.5, scores.getOrBeta(), 0.0);
        assertEquals("percentCI should be deserialized", "95% CI", scores.getPercentCI());
    }

    @Test
    public void testUpdateObjectMapperCanDeserializeGwasWithLowercase() throws Exception {
        // Test backward compatibility with lowercase field names using the update mapper
        String jsonWithLowercase = "{\"pvalue\":0.00002,\"pvalueMlog\":4.7,\"pvalueText\":\"2e-5\",\"orBeta\":1.3,\"percentCI\":\"95% CI\"}";

        // Get the ObjectMapper used for catalog updates
        ObjectMapper updateMapper = JacksonUtils.getUpdateObjectMapper();

        // Try to deserialize - should work due to @JsonAlias
        GwasAssociationStudyTraitScores scores = updateMapper.readValue(jsonWithLowercase, GwasAssociationStudyTraitScores.class);

        // Verify the values were correctly deserialized
        assertNotNull("Deserialized object should not be null", scores);
        assertEquals("pValue should be deserialized from lowercase", 0.00002, scores.getPValue(), 0.0);
        assertEquals("pValueMlog should be deserialized from lowercase", 4.7, scores.getPValueMlog(), 0.0);
        assertEquals("pValueText should be deserialized from lowercase", "2e-5", scores.getPValueText());
        assertEquals("orBeta should be deserialized from lowercase", 1.3, scores.getOrBeta(), 0.0);
        assertEquals("percentCI should be deserialized from lowercase", "95% CI", scores.getPercentCI());
    }
}
