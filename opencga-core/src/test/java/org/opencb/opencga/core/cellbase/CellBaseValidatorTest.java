package org.opencb.opencga.core.cellbase;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.IOException;

@Category(ShortTests.class)
public class CellBaseValidatorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Test
    public void testValidateFixVersion() throws IOException {
        CellBaseConfiguration c = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "5.2", "3", null), "hsapiens", "grch38", true);
        Assert.assertEquals("v5.2", c.getVersion());
        Assert.assertEquals("3", c.getDataRelease());

        c = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "6.7", "1", null), "hsapiens", "grch38", true);
        Assert.assertEquals("v6.7", c.getVersion());
        Assert.assertEquals("1", c.getDataRelease());
    }

    @Test
    public void testValidateUndefinedDataRelease() throws IOException {
        CellBaseConfiguration c = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.2", null, null), "hsapiens", "grch38", true);
        Assert.assertEquals("v5.2", c.getVersion());
        Assert.assertNotNull(c.getDataRelease());

        c = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v6.7", null, null), "hsapiens", "grch38", true);
        Assert.assertEquals("v6.7", c.getVersion());
        Assert.assertNotNull(c.getDataRelease());
    }

    @Test
    public void testInvalidUrlPath() throws IOException {
        thrown.expectMessage("Unable to access cellbase url");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL+"/NONEXISTING/", "v5.2", null, null), "green alien", "grch84", true);
    }

    @Test
    public void testInvalidUrl() throws IOException {
        thrown.expectMessage("Unable to access cellbase url");
        CellBaseValidator.validate(new CellBaseConfiguration("https://ws.zettagenomics-NONEXISTING.com/cellbase/NONEXISTING/", "v5.2", null, null), "green alien", "grch84", true);
    }

    @Test
    public void testInvalidVersion() throws IOException {
        thrown.expectMessage("Unable to access cellbase url");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "2", null, null), "green alien", "grch84", true);
    }

    @Test
    public void testInvalidSpecies() throws IOException {
        thrown.expectMessage("Species 'galien' not found in cellbase");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.2", null, null), "green alien", "grch84", true);
    }

    @Test
    public void testInvalidAssembly() throws IOException {
        thrown.expectMessage("Assembly 'grch84' not found in cellbase");
        thrown.expectMessage("Supported assemblies : [GRCh38, GRCh37]");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.2", null, null), "homo sapiens", "grch84", true);
    }

    @Test
    public void testInvalidDR() throws IOException {
        thrown.expectMessage("DataRelease 'INVALID_DR' not found on cellbase");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.2", "INVALID_DR", null), "hsapiens", "grch38", true);
    }

    @Test
    public void testNoActiveReleases() throws IOException {
        thrown.expectMessage("No active data releases found on cellbase");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.8", null, null), "scerevisiae", "R64-1-1", true);
    }

    @Test
    public void testApiKey() throws IOException {
        String apiKey = System.getenv("CELLBASE_HGMD_APIKEY");
        Assume.assumeTrue(StringUtils.isNotEmpty(apiKey));
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.8", null, apiKey), "hsapiens", "grch38", true);
        Assert.assertNotNull(validated.getApiKey());
    }

    @Test
    public void testApiKeyNotSupported() throws IOException {
        String apiKey = System.getenv("CELLBASE_HGMD_APIKEY");
        Assume.assumeTrue(StringUtils.isNotEmpty(apiKey));
        thrown.expectMessage("API key not supported");
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.2", null, apiKey), "hsapiens", "grch38", true);
        Assert.assertNotNull(validated.getApiKey());
    }

    @Test
    public void testApiKeyEmpty() throws IOException {
        String apiKey = "";
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.2", null, apiKey), "hsapiens", "grch38", true);
        Assert.assertNull(validated.getApiKey());
    }

    @Test
    public void testMalformedApiKey() throws IOException {
        thrown.expectMessage("Malformed API key for cellbase");
        String apiKey = "MALFORMED_API_KEY";
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.8", null, apiKey), "hsapiens", "grch38", true);
        Assert.assertNotNull(validated.getApiKey());
    }

    @Test
    public void testUnsignedApiKey() throws IOException {
        thrown.expectMessage("Invalid API key for cellbase");
        String apiKey = "eyJhbGciOiJIUzI1NiJ9.eyJzb3VyY2VzIjp7ImhnbWQiOjkyMjMzNzIwMzY4NTQ3NzU4MDd9LCJ2ZXJzaW9uIjoiMS4wIiwic3ViIjoiWkVUVEEiLCJpYXQiOjE2OTMyMTY5MDd9.invalidsignature";
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.8", null, apiKey), "hsapiens", "grch38", true);
        Assert.assertNotNull(validated.getApiKey());
    }

}