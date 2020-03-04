package org.opencb.opencga.analysis.variant.geneticChecks;

import org.junit.Test;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.RelatednessReport;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

public class GeneticChecksUtilsTest {

    @Test
    public void buildRelatednessReport() throws ToolException, IOException {

        URI resourceUri = getResourceUri("ibd.genome");
        File file = Paths.get(resourceUri.getPath()).toFile();
        RelatednessReport relatednessReport = GeneticChecksUtils.buildRelatednessReport(file);

        System.out.println(JacksonUtils.getDefaultNonNullObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(relatednessReport));
    }
}