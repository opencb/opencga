package org.opencb.opencga.core.cellbase;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;

import java.io.IOException;

public class CellBaseValidatorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Test
    public void testValidateFixVersion() throws IOException {
        CellBaseConfiguration c = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "5.1", "2", null), "hsapiens", "grch38", true);
        Assert.assertEquals("v5.1", c.getVersion());
    }

    @Test
    public void testValidateUndefinedDataRelease() throws IOException {
        CellBaseConfiguration c = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.1", null, null), "hsapiens", "grch38", true);
        Assert.assertEquals("v5.1", c.getVersion());
        Assert.assertNotNull(c.getDataRelease());
    }

    @Test
    public void testInvalidUrlPath() throws IOException {
        thrown.expectMessage("Unable to access cellbase url");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL+"/NONEXISTING/", "v5.1", null, null), "green alien", "grch84", true);
    }

    @Test
    public void testInvalidUrl() throws IOException {
        thrown.expectMessage("Unable to access cellbase url");
        CellBaseValidator.validate(new CellBaseConfiguration("https://ws.zettagenomics-NONEXISTING.com/cellbase/NONEXISTING/", "v5.1", null, null), "green alien", "grch84", true);
    }

    @Test
    public void testInvalidVersion() throws IOException {
        thrown.expectMessage("Unable to access cellbase url");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "2", null, null), "green alien", "grch84", true);
    }

    @Test
    public void testInvalidSpecies() throws IOException {
        thrown.expectMessage("Species 'galien' not found in cellbase");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.1", null, null), "green alien", "grch84", true);
    }

    @Test
    public void testInvalidAssembly() throws IOException {
        thrown.expectMessage("Assembly 'grch84' not found in cellbase");
        thrown.expectMessage("Supported assemblies : [GRCh38, GRCh37]");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.1", null, null), "homo sapiens", "grch84", true);
    }

    @Test
    public void testInvalidDR() throws IOException {
        thrown.expectMessage("DataRelease 'INVALID_DR' not found on cellbase");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.1", "INVALID_DR", null), "hsapiens", "grch38", true);
    }

    @Test
    public void testNoActiveReleases() throws IOException {
        thrown.expectMessage("No active data releases found on cellbase");
        CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.1", null, null), "mmusculus", "GRCm38", true);
    }

    @Test
    public void testToken() throws IOException {
        String token = System.getenv("CELLBASE_HGMD_TOKEN");
        Assume.assumeTrue(StringUtils.isNotEmpty(token));
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.4", null, token), "hsapiens", "grch38", true);
        Assert.assertNotNull(validated.getToken());
    }

    @Test
    public void testTokenNotSupported() throws IOException {
        String token = System.getenv("CELLBASE_HGMD_TOKEN");
        Assume.assumeTrue(StringUtils.isNotEmpty(token));
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.1", null, token), "hsapiens", "grch38", true);
        Assert.assertNotNull(validated.getToken());
    }

    @Test
    public void testMalformedToken() throws IOException {
        thrown.expectMessage("Malformed token for cellbase");
        String token = "MALFORMED_TOKEN";
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.4", null, token), "hsapiens", "grch38", true);
        Assert.assertNotNull(validated.getToken());
    }

    @Test
    public void testUnsignedToken() throws IOException {
        thrown.expectMessage("Invalid token for cellbase");
        String token = "eyJhbGciOiJIUzI1NiJ9.eyJzb3VyY2VzIjp7ImhnbWQiOjkyMjMzNzIwMzY4NTQ3NzU4MDd9LCJ2ZXJzaW9uIjoiMS4wIiwic3ViIjoiWkVUVEEiLCJpYXQiOjE2OTMyMTY5MDd9.invalidsignature";
        CellBaseConfiguration validated = CellBaseValidator.validate(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v5.4", null, token), "hsapiens", "grch38", true);
        Assert.assertNotNull(validated.getToken());
    }

}