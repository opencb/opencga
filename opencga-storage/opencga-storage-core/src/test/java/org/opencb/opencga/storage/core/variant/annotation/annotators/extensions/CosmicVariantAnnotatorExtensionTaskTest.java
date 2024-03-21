package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.opencb.biodata.models.common.DataVersion;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic.CosmicVariantAnnotatorExtensionTask;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CosmicVariantAnnotatorExtensionTaskTest {

    private final String COSMIC_VERSION = "v95";

    @Test
    public void testSetupCosmicVariantAnnotatorExtensionTask() throws Exception {
        ObjectMap options = new ObjectMap();
        CosmicVariantAnnotatorExtensionTask task = new CosmicVariantAnnotatorExtensionTask(options);

        Assert.assertEquals(false, task.isAvailable());

        // Setup COSMIC directory
        Path cosmicPath = initCosmicPath();

        // Set-up COSMIC variant annotator extension task, once
        task.setup(cosmicPath.toUri());

        // Set-up COSMIC variant annotator extension task, twice
        task.setup(cosmicPath.toUri());

        ObjectMap metadata = task.getMetadata();
        Assert.assertEquals(COSMIC_VERSION, metadata.get("version"));

        Assert.assertEquals(true, task.isAvailable());
    }

    @Test
    public void testSCosmicVariantAnnotatorExtensionTask() {
        ObjectMap options = new ObjectMap();
        CosmicVariantAnnotatorExtensionTask task = new CosmicVariantAnnotatorExtensionTask(options);
        Assert.assertEquals(false, task.isAvailable());
    }

    @Test
    public void testAnnotationCosmicVariantAnnotatorExtensionTask() throws Exception {
        ObjectMap options = new ObjectMap();
        CosmicVariantAnnotatorExtensionTask task = new CosmicVariantAnnotatorExtensionTask(options);

        Assert.assertEquals(false, task.isAvailable());

        // Setup COSMIC directory
        Path cosmicPath = initCosmicPath();

        // Set-up COSMIC variant annotator extension task, once
        task.setup(cosmicPath.toUri());

        List<VariantAnnotation> inputVariantAnnotations = new ArrayList<>();
        VariantAnnotation variantAnnotation1 = new VariantAnnotation();
        variantAnnotation1.setChromosome("12");
        variantAnnotation1.setStart(124402657);
        variantAnnotation1.setEnd(124402657);
        variantAnnotation1.setReference("G");
        variantAnnotation1.setAlternate("T");
        inputVariantAnnotations.add(variantAnnotation1);
        VariantAnnotation variantAnnotation2 = new VariantAnnotation();
        variantAnnotation2.setChromosome("22");
        variantAnnotation2.setStart(124402657);
        variantAnnotation2.setEnd(124402657);
        variantAnnotation2.setReference("G");
        variantAnnotation2.setAlternate("T");
        inputVariantAnnotations.add(variantAnnotation2);

        List<VariantAnnotation> outputVariantAnnotations = task.apply(inputVariantAnnotations);
        task.post();

        Assert.assertEquals(inputVariantAnnotations.size(), outputVariantAnnotations.size());

        // Checking variantAnnotation1
        Assert.assertEquals(1, outputVariantAnnotations.get(0).getTraitAssociation().size());
        Assert.assertEquals("COSV62300079", outputVariantAnnotations.get(0).getTraitAssociation().get(0).getId());
        Assert.assertEquals("liver", outputVariantAnnotations.get(0).getTraitAssociation().get(0).getSomaticInformation().getPrimarySite());
        Assert.assertEquals("hepatocellular carcinoma", outputVariantAnnotations.get(0).getTraitAssociation().get(0).getSomaticInformation().getHistologySubtype());
        Assert.assertEquals("PMID:323", outputVariantAnnotations.get(0).getTraitAssociation().get(0).getBibliography().get(0));

        // Checking variantAnnotation2
        Assert.assertTrue(CollectionUtils.isEmpty(outputVariantAnnotations.get(1).getTraitAssociation()));
    }

    private Path initCosmicPath() throws IOException {
        Path cosmicPath = getTempPath();
        if (!cosmicPath.toFile().mkdirs()) {
            throw new IOException("Error creating the COSMIC path: " + cosmicPath.toAbsolutePath());
        }
        Path cosmicFile = Paths.get(getClass().getResource("/custom_annotation/cosmic.small.tsv.gz").getPath());
        DataVersion cosmicDataVersion = new DataVersion("variant", "cosmic", COSMIC_VERSION, "20231212",
                "hsapiens", "GRCh38", Collections.singletonList(cosmicFile.getFileName().toString()),
                Collections.singletonList("http://cosmic.org"), null);
        JacksonUtils.getDefaultObjectMapper().writeValue(cosmicPath.resolve(CosmicVariantAnnotatorExtensionTask.COSMIC_VERSION_FILENAME).toFile(), cosmicDataVersion);
        Files.copy(cosmicFile, cosmicPath.resolve(cosmicDataVersion.getFiles().get(0)));

        return cosmicPath;
    }

    private Path getTempPath() {
        return Paths.get("target/test-data").resolve(TimeUtils.getTimeMillis() + "_" + RandomStringUtils.random(8, true, false));
    }
}