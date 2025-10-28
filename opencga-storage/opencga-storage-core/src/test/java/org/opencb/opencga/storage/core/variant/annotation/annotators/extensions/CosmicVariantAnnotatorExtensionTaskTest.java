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
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationExtensionConfigureParams;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic.CosmicVariantAnnotatorExtensionTask;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic.CosmicVariantAnnotatorExtensionTask.*;

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

        CosmicVariantAnnotatorExtensionTask task = new CosmicVariantAnnotatorExtensionTask(null);
        Assert.assertEquals(false, task.isAvailable());

        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams();
        params.setExtension(CosmicVariantAnnotatorExtensionTask.ID);
        params.setResources(Collections.singletonList(cosmicFile.toAbsolutePath().toString()));
        ObjectMap cosmicParams = new ObjectMap();
        cosmicParams.put(CosmicVariantAnnotatorExtensionTask.COSMIC_VERSION_KEY, COSMIC_VERSION);
        cosmicParams.put(CosmicVariantAnnotatorExtensionTask.COSMIC_ASSEMBLY_KEY, COSMIC_ASSEMBLY);
        params.setParams(cosmicParams);

        task.setup(params, null);

        ObjectMap metadata = task.getMetadata();
        Assert.assertEquals(COSMIC_VERSION, metadata.get(COSMIC_VERSION_KEY));
        Assert.assertEquals(CosmicVariantAnnotatorExtensionTask.ID, metadata.get(NAME_KEY));
        Assert.assertEquals(COSMIC_ASSEMBLY, metadata.get(COSMIC_ASSEMBLY_KEY));
        Assert.assertTrue(metadata.containsKey(INDEX_CREATION_DATE_KEY));
        Assert.assertTrue(StringUtils.isNotEmpty(metadata.getString(INDEX_CREATION_DATE_KEY)));

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
//        Path outPath = getTempPath();
//        if (!outPath.toFile().mkdirs()) {
//            throw new IOException("Error creating the output path: " + outPath.toAbsolutePath());
//        }
//        System.out.println("outPath = " + outPath.toAbsolutePath());

        // Setup COSMIC directory
        Path cosmicFile = initCosmicPath();
        System.out.println("cosmicFile = " + cosmicFile.toAbsolutePath());

        CosmicVariantAnnotatorExtensionTask task = new CosmicVariantAnnotatorExtensionTask(null);
        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams();
        params.setExtension(CosmicVariantAnnotatorExtensionTask.ID);
        params.setResources(Collections.singletonList(cosmicFile.toAbsolutePath().toString()));
        ObjectMap cosmicParams = new ObjectMap();
        cosmicParams.put(CosmicVariantAnnotatorExtensionTask.COSMIC_VERSION_KEY, COSMIC_VERSION);
        cosmicParams.put(CosmicVariantAnnotatorExtensionTask.COSMIC_ASSEMBLY_KEY, COSMIC_ASSEMBLY);
        params.setParams(cosmicParams);

        // Cosmic index is built next to the COSMIC tar.gz file
        task.setup(params, null);

        Assert.assertEquals(true, task.isAvailable());
        ObjectMap options = new ObjectMap();

        // All is ready, so we can use the factory to get the task
        options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), CosmicVariantAnnotatorExtensionTask.ID);
        options.putAll(task.getOptions());

        task = (CosmicVariantAnnotatorExtensionTask) new VariantAnnotatorExtensionsFactory().getVariantAnnotatorExtensions(options).get(0);
        task.pre();

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

    private static Path initCosmicPath() throws IOException {
        return initCosmicPath(getTempPath());
    }

    public static Path initCosmicPath(Path cosmicPath) throws IOException {
        if (!Files.isDirectory(cosmicPath) && !cosmicPath.toFile().mkdirs()) {
            throw new IOException("Error creating the COSMIC path: " + cosmicPath.toAbsolutePath());
        }

        String cosmicFilename = "Small_Cosmic_" + COSMIC_VERSION + "_" + COSMIC_ASSEMBLY + ".tar.gz";
        Path targetPath = Paths.get(VariantStorageBaseTest.getResourceUri("custom_annotation/" + cosmicFilename, cosmicPath));

        if (!Files.exists(targetPath)) {
            throw new IOException("Error copying COSMIC file to " + targetPath);
        }

        return targetPath;
    }

    public static Path initInvalidCosmicPath() throws IOException {
        return initInvalidCosmicPath(getTempPath());
    }

    public static Path initInvalidCosmicPath(Path cosmicPath) throws IOException {
        if (!Files.isDirectory(cosmicPath) && !cosmicPath.toFile().mkdirs()) {
            throw new IOException("Error creating the COSMIC path: " + cosmicPath.toAbsolutePath());
        }
        Path targetPath = Paths.get(VariantStorageBaseTest.getResourceUri("custom_annotation/myannot.vcf", cosmicPath));

        if (!Files.exists(targetPath)) {
            throw new IOException("Error copying COSMIC file to " + targetPath);
        }

        return targetPath;
    }

    private static Path getTempPath() {
        return Paths.get("target/test-data").resolve(TimeUtils.getTimeMillis() + "_" + RandomStringUtils.random(8, true, false));
    }
}