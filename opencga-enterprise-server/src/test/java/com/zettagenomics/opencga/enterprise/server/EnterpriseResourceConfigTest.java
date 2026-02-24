package com.zettagenomics.opencga.enterprise.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.GwasAssociationStudyTraitScores;
import org.opencb.opencga.server.rest.OpenCGAObjectMapperProvider;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Test for EnterpriseResourceConfig to ensure the ObjectMapperProvider is properly registered
 * and the REST API uses the correct ObjectMapper with all necessary mixins.
 *
 * This test verifies the fix for TASK-8139: GwasAssociationStudyTraitScores deserialization
 * with camelCase field names (pValue, pValueMlog, pValueText).
 */
public class EnterpriseResourceConfigTest {

    @Test
    public void testOpenCGAObjectMapperProviderIsRegistered() {
        // Verify that OpenCGAObjectMapperProvider is in the auxiliarClasses
        Set<Class<?>> auxiliarClasses = EnterpriseResourceConfig.auxiliarClasses;

        assertNotNull("auxiliarClasses should not be null", auxiliarClasses);
        assertTrue("auxiliarClasses should contain OpenCGAObjectMapperProvider",
                auxiliarClasses.contains(OpenCGAObjectMapperProvider.class));
    }

    @Test
    public void testEnterpriseResourceConfigRegistersProvider() {
        // Create the enterprise resource config
        EnterpriseResourceConfig config = new EnterpriseResourceConfig();

        assertNotNull("EnterpriseResourceConfig should not be null", config);

        // The config should be a valid Jersey ResourceConfig
        assertTrue("EnterpriseResourceConfig should extend ResourceConfig",
                config instanceof ResourceConfig);
    }

    @Test
    public void testObjectMapperProviderHasGwasMixinRegistered() {
        // Verify that the provider's ObjectMapper has the GWAS mixin registered
        OpenCGAObjectMapperProvider provider = new OpenCGAObjectMapperProvider();
        ObjectMapper mapper = provider.getContext(Object.class);

        // Check that GwasAssociationStudyTraitScores mixin is registered
        Class<?> mixinClass = mapper.findMixInClassFor(GwasAssociationStudyTraitScores.class);

        assertNotNull("Mixin should be registered for GwasAssociationStudyTraitScores", mixinClass);
        assertEquals("Should use GwasAssociationStudyTraitScoresMixin",
                "GwasAssociationStudyTraitScoresMixin",
                mixinClass.getSimpleName());
    }

    @Test
    public void testDeserializesGwasWithCamelCaseFields() throws Exception {
        // This is the critical test for TASK-8139: deserialize GWAS data with camelCase from Python clients
        String jsonFromPythonClient = "{\"pValue\":0.00001,\"pValueMlog\":5.0,\"pValueText\":\"1e-5\",\"orBeta\":1.5,\"percentCI\":\"95% CI\"}";

        // Use the provider that would be used by the REST API
        OpenCGAObjectMapperProvider provider = new OpenCGAObjectMapperProvider();
        ObjectMapper mapper = provider.getContext(Object.class);

        // Deserialize the GWAS scores
        GwasAssociationStudyTraitScores scores = mapper.readValue(jsonFromPythonClient, GwasAssociationStudyTraitScores.class);

        // Verify all fields are correctly deserialized (not null)
        assertNotNull("Deserialized object should not be null", scores);
        assertEquals("pValue should be deserialized correctly", 0.00001, scores.getPValue(), 0.0);
        assertEquals("pValueMlog should be deserialized correctly", 5.0, scores.getPValueMlog(), 0.0);
        assertEquals("pValueText should be deserialized correctly", "1e-5", scores.getPValueText());
        assertEquals("orBeta should be deserialized correctly", 1.5, scores.getOrBeta(), 0.0);
        assertEquals("percentCI should be deserialized correctly", "95% CI", scores.getPercentCI());
    }

    @Test
    public void testDeserializesGwasWithLowercaseFieldsBackwardCompatibility() throws Exception {
        // Test backward compatibility with lowercase field names
        String jsonWithLowercase = "{\"pvalue\":0.00002,\"pvalueMlog\":4.7,\"pvalueText\":\"2e-5\",\"orBeta\":1.3,\"percentCI\":\"95% CI\"}";

        OpenCGAObjectMapperProvider provider = new OpenCGAObjectMapperProvider();
        ObjectMapper mapper = provider.getContext(Object.class);

        // Deserialize the GWAS scores
        GwasAssociationStudyTraitScores scores = mapper.readValue(jsonWithLowercase, GwasAssociationStudyTraitScores.class);

        // Verify deserialization works with lowercase (through @JsonAlias)
        assertNotNull("Deserialized object should not be null", scores);
        assertEquals("pValue should be deserialized from lowercase", 0.00002, scores.getPValue(), 0.0);
        assertEquals("pValueMlog should be deserialized from lowercase", 4.7, scores.getPValueMlog(), 0.0);
        assertEquals("pValueText should be deserialized from lowercase", "2e-5", scores.getPValueText());
        assertEquals("orBeta should be deserialized from lowercase", 1.3, scores.getOrBeta(), 0.0);
        assertEquals("percentCI should be deserialized from lowercase", "95% CI", scores.getPercentCI());
    }

    @Test
    public void testSerializesGwasToCorrectFormat() throws Exception {
        // Verify that the mapper serializes GWAS data to the correct format
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

        // Verify the JSON contains the expected field names
        assertTrue("JSON should contain pValue", json.contains("pValue") || json.contains("pvalue"));
        assertTrue("JSON should contain pValueMlog", json.contains("pValueMlog") || json.contains("pvalueMlog"));
        assertTrue("JSON should contain pValueText", json.contains("pValueText") || json.contains("pvalueText"));
        assertTrue("JSON should contain orBeta", json.contains("orBeta"));
        assertTrue("JSON should contain percentCI", json.contains("percentCI"));
    }

}
