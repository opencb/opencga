package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic.CosmicVariantAnnotatorExtensionTask;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Category(ShortTests.class)
public class CosmicVariantAnnotatorExtensionTaskTest {

    public static final String COSMIC_ASSEMBLY ="GRCh38";
    public static final String COSMIC_VERSION = "v101";

    @Test
    public void testSetupCosmicVariantAnnotatorExtensionTask() throws Exception {
        Path outPath = getTempPath();
        if (!outPath.toFile().mkdirs()) {
            throw new IOException("Error creating the output path: " + outPath.toAbsolutePath());
        }
        System.out.println("outPath = " + outPath.toAbsolutePath());

        // Setup COSMIC directory
        Path cosmicFile = initCosmicPath();
        System.out.println("cosmicFile = " + cosmicFile.toAbsolutePath());

        ObjectMap options = new ObjectMap();
        options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), cosmicFile);
        options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), COSMIC_VERSION);
        options.put(VariantStorageOptions.ASSEMBLY.key(), COSMIC_ASSEMBLY);

        CosmicVariantAnnotatorExtensionTask task = new CosmicVariantAnnotatorExtensionTask(options);

        Assert.assertEquals(false, task.isAvailable());

        // Set-up COSMIC variant annotator extension task, once
        task.setup(outPath.toUri());

        // Set-up COSMIC variant annotator extension task, twice
        task.setup(outPath.toUri());

        ObjectMap metadata = task.getMetadata();
        Assert.assertEquals(COSMIC_VERSION, metadata.get("version"));
        Assert.assertEquals(CosmicVariantAnnotatorExtensionTask.ID, metadata.get("data"));
        Assert.assertEquals(COSMIC_ASSEMBLY, metadata.get("assembly"));

        Assert.assertEquals(true, task.isAvailable());
    }

    @Test
    public void testSCosmicVariantAnnotatorExtensionTask() {
        ObjectMap options = new ObjectMap();
        CosmicVariantAnnotatorExtensionTask task = new CosmicVariantAnnotatorExtensionTask(options);
        Assert.assertEquals(false, task.isAvailable());
    }

    @Test
    public void testAnnotationCosmicVariantAnnotatorExtensionTaskUsingFactory() throws Exception {
        Path outPath = getTempPath();
        if (!outPath.toFile().mkdirs()) {
            throw new IOException("Error creating the output path: " + outPath.toAbsolutePath());
        }
        System.out.println("outPath = " + outPath.toAbsolutePath());

        // Setup COSMIC directory
        Path cosmicFile = initCosmicPath();
        System.out.println("cosmicFile = " + cosmicFile.toAbsolutePath());

        ObjectMap options = new ObjectMap();
        options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), cosmicFile);
        options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), COSMIC_VERSION);
        options.put(VariantStorageOptions.ASSEMBLY.key(), COSMIC_ASSEMBLY);
        options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), CosmicVariantAnnotatorExtensionTask.ID);

        CosmicVariantAnnotatorExtensionTask task = (CosmicVariantAnnotatorExtensionTask) new VariantAnnotatorExtensionsFactory().getVariantAnnotatorExtensions(options).get(0);

        Assert.assertEquals(false, task.isAvailable());

        // Set-up COSMIC variant annotator extension task, once
        task.setup(outPath.toUri());

        List<VariantAnnotation> inputVariantAnnotations = new ArrayList<>();
        VariantAnnotation variantAnnotation1 = new VariantAnnotation();
        variantAnnotation1.setChromosome("6");
        variantAnnotation1.setStart(25864933);
        variantAnnotation1.setEnd(25864933);
        variantAnnotation1.setReference("G");
        variantAnnotation1.setAlternate("A");
        inputVariantAnnotations.add(variantAnnotation1);
        VariantAnnotation variantAnnotation2 = new VariantAnnotation();
        variantAnnotation2.setChromosome("8");
        variantAnnotation2.setStart(107264278);
        variantAnnotation2.setEnd(107264278);
        variantAnnotation2.setReference("T");
        variantAnnotation2.setAlternate("G");
        inputVariantAnnotations.add(variantAnnotation2);

        List<VariantAnnotation> outputVariantAnnotations = task.apply(inputVariantAnnotations);
        task.post();

        Assert.assertEquals(inputVariantAnnotations.size(), outputVariantAnnotations.size());

        // Checking variantAnnotation1
        Assert.assertEquals(1, outputVariantAnnotations.get(0).getTraitAssociation().size());
        Assert.assertEquals("COSV57759629", outputVariantAnnotations.get(0).getTraitAssociation().get(0).getId());
        Assert.assertEquals("haematopoietic and lymphoid tissue", outputVariantAnnotations.get(0).getTraitAssociation().get(0).getSomaticInformation().getPrimarySite());
        Assert.assertTrue(StringUtils.isEmpty(outputVariantAnnotations.get(0).getTraitAssociation().get(0).getSomaticInformation().getHistologySubtype()));
        Assert.assertEquals("lymphoid neoplasm", outputVariantAnnotations.get(0).getTraitAssociation().get(0).getSomaticInformation().getPrimaryHistology());
        Assert.assertTrue(CollectionUtils.isEmpty(outputVariantAnnotations.get(0).getTraitAssociation().get(0).getBibliography()));

        // Checking variantAnnotation2
        Assert.assertEquals(1, outputVariantAnnotations.get(1).getTraitAssociation().size());
        Assert.assertEquals("COSV108830958", outputVariantAnnotations.get(1).getTraitAssociation().get(0).getId());
        Assert.assertEquals("thyroid", outputVariantAnnotations.get(1).getTraitAssociation().get(0).getSomaticInformation().getPrimarySite());
        Assert.assertEquals("papillary carcinoma", outputVariantAnnotations.get(1).getTraitAssociation().get(0).getSomaticInformation().getHistologySubtype());
        Assert.assertEquals("PMID:33888599", outputVariantAnnotations.get(1).getTraitAssociation().get(0).getBibliography().get(0));
    }

    public static Path initCosmicPath() throws IOException {
        Path cosmicPath = getTempPath();
        if (!cosmicPath.toFile().mkdirs()) {
            throw new IOException("Error creating the COSMIC path: " + cosmicPath.toAbsolutePath());
        }
        Path cosmicFile = Paths.get(CosmicVariantAnnotatorExtensionTaskTest.class.getResource("/custom_annotation/Small_Cosmic_"
                + COSMIC_VERSION + "_" + COSMIC_ASSEMBLY + ".tar.gz").getPath());
        Path targetPath = cosmicPath.resolve(cosmicFile.getFileName());
        Files.copy(cosmicFile, targetPath);

        if (!Files.exists(targetPath)) {
            throw new IOException("Error copying COSMIC file to " + targetPath);
        }

        return targetPath;
    }

    public static Path initInvalidCosmicPath() throws IOException {
        Path cosmicPath = getTempPath();
        if (!cosmicPath.toFile().mkdirs()) {
            throw new IOException("Error creating the COSMIC path: " + cosmicPath.toAbsolutePath());
        }
        Path cosmicFile = Paths.get(CosmicVariantAnnotatorExtensionTaskTest.class.getResource("/custom_annotation/myannot.vcf").getPath());
        Path targetPath = cosmicPath.resolve(cosmicFile.getFileName());
        Files.copy(cosmicFile, targetPath);

        if (!Files.exists(targetPath)) {
            throw new IOException("Error copying COSMIC file to " + targetPath);
        }

        return targetPath;
    }

    public static Path getTempPath() {
        return Paths.get("target/test-data").resolve(TimeUtils.getTimeMillis() + "_" + RandomStringUtils.random(8, true, false));
    }
}