package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationExtensionConfigureParams;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic.CosmicVariantAnnotatorExtensionTask;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.hgmd.HgmdVariantAnnotatorExtensionTask;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic.CosmicVariantAnnotatorExtensionTask.*;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.hgmd.HgmdVariantAnnotatorExtensionTask.HGMD_ASSEMBLY_KEY;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.hgmd.HgmdVariantAnnotatorExtensionTask.HGMD_VERSION_KEY;

@Category(ShortTests.class)
public class HgmdVariantAnnotatorExtensionTaskTest {

    public static final String HGMD_ASSEMBLY ="hg38";
    public static final String HGMD_VERSION = "2020.3";

    @Test
    public void testSetupHgmdVariantAnnotatorExtensionTask() throws Exception {
        Path outPath = getTempPath();
        if (!outPath.toFile().mkdirs()) {
            throw new IOException("Error creating the output path: " + outPath.toAbsolutePath());
        }
        System.out.println("outPath = " + outPath.toAbsolutePath());

        // Setup HGMD directory
        Path hgmdFile = initHgmdPath();
        System.out.println("hgmdFile = " + hgmdFile.toAbsolutePath());

        HgmdVariantAnnotatorExtensionTask task = new HgmdVariantAnnotatorExtensionTask(null);
        Assert.assertEquals(false, task.isAvailable());

        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams();
        params.setExtension(HgmdVariantAnnotatorExtensionTask.ID);
        params.setResources(Collections.singletonList(hgmdFile.toAbsolutePath().toString()));
        ObjectMap hgmdParams = new ObjectMap();
        hgmdParams.put(HGMD_VERSION_KEY, HGMD_VERSION);
        hgmdParams.put(HGMD_ASSEMBLY_KEY, HGMD_ASSEMBLY);
        params.setParams(hgmdParams);

        task.setup(params, null);

        ObjectMap metadata = task.getMetadata();
        Assert.assertEquals(HGMD_VERSION, metadata.get(HGMD_VERSION_KEY));
        Assert.assertEquals(HgmdVariantAnnotatorExtensionTask.ID, metadata.get(NAME_KEY));
        Assert.assertEquals(HGMD_ASSEMBLY, metadata.get(HGMD_ASSEMBLY_KEY));
        Assert.assertTrue(metadata.containsKey(INDEX_CREATION_DATE_KEY));
        Assert.assertTrue(StringUtils.isNotEmpty(metadata.getString(INDEX_CREATION_DATE_KEY)));

        Assert.assertEquals(true, task.isAvailable());
    }

    @Test
    public void testHgmdVariantAnnotatorExtensionTask() {
        ObjectMap options = new ObjectMap();
        HgmdVariantAnnotatorExtensionTask task = new HgmdVariantAnnotatorExtensionTask(options);
        Assert.assertEquals(false, task.isAvailable());
    }

    @Test
    public void testAnnotationHgmdVariantAnnotatorExtensionTaskUsingFactory() throws Exception {
        // Setup HGMD directory
        Path hgmdFile = initHgmdPath();
        System.out.println("hgmdFile = " + hgmdFile.toAbsolutePath());

        HgmdVariantAnnotatorExtensionTask task = new HgmdVariantAnnotatorExtensionTask(null);
        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams();
        params.setExtension(HgmdVariantAnnotatorExtensionTask.ID);
        params.setResources(Collections.singletonList(hgmdFile.toAbsolutePath().toString()));
        ObjectMap hgmdParams = new ObjectMap();
        hgmdParams.put(HgmdVariantAnnotatorExtensionTask.HGMD_VERSION_KEY, HGMD_VERSION);
        hgmdParams.put(HgmdVariantAnnotatorExtensionTask.HGMD_ASSEMBLY_KEY, HGMD_ASSEMBLY);
        params.setParams(hgmdParams);

        // HGMD index is built next to the HGMD file
        task.setup(params, null);

        Assert.assertEquals(true, task.isAvailable());
        ObjectMap options = new ObjectMap();

        // All is ready, so we can use the factory to get the task
        options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), HgmdVariantAnnotatorExtensionTask.ID);
        options.putAll(task.getOptions());

        task = (HgmdVariantAnnotatorExtensionTask) new VariantAnnotatorExtensionsFactory().getVariantAnnotatorExtensions(options).get(0);
        task.pre();

        List<VariantAnnotation> inputVariantAnnotations = new ArrayList<>();

        Variant variant = new Variant("1", 930215, 930215, "A", "G");
        List<Variant> normalizedVariants = normalizeVariant(variant);
        Assert.assertEquals(1, normalizedVariants.size());
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setChromosome(normalizedVariants.get(0).getChromosome());
        variantAnnotation.setStart(normalizedVariants.get(0).getStart());
        variantAnnotation.setEnd(normalizedVariants.get(0).getEnd());
        variantAnnotation.setReference(normalizedVariants.get(0).getReference());
        variantAnnotation.setAlternate(normalizedVariants.get(0).getAlternate());
        inputVariantAnnotations.add(variantAnnotation);

        variant = new Variant("1", 11792370, 11792370, "C", "CT");
        normalizedVariants = normalizeVariant(variant);
        Assert.assertEquals(1, normalizedVariants.size());
        variantAnnotation = new VariantAnnotation();
        variantAnnotation.setChromosome(normalizedVariants.get(0).getChromosome());
        variantAnnotation.setStart(normalizedVariants.get(0).getStart());
        variantAnnotation.setEnd(normalizedVariants.get(0).getEnd());
        variantAnnotation.setReference(normalizedVariants.get(0).getReference());
        variantAnnotation.setAlternate(normalizedVariants.get(0).getAlternate());
        inputVariantAnnotations.add(variantAnnotation);

        List<VariantAnnotation> outputVariantAnnotations = task.apply(inputVariantAnnotations);
        task.post();

        Assert.assertEquals(inputVariantAnnotations.size(), outputVariantAnnotations.size());

        // Checking variantAnnotation1
        variantAnnotation = outputVariantAnnotations.get(0);
        Assert.assertEquals(1, variantAnnotation.getTraitAssociation().size());
        Assert.assertEquals(HgmdVariantAnnotatorExtensionTask.ID, variantAnnotation.getTraitAssociation().get(0).getSource().getName());
        Assert.assertEquals(HGMD_VERSION, variantAnnotation.getTraitAssociation().get(0).getSource().getVersion());
        Assert.assertEquals(HGMD_ASSEMBLY, variantAnnotation.getTraitAssociation().get(0).getAssembly());
        Assert.assertEquals("CM1613956", variantAnnotation.getTraitAssociation().get(0).getId());
        Assert.assertEquals("Retinitis_pigmentosa", variantAnnotation.getTraitAssociation().get(0).getHeritableTraits().get(0).getTrait());

        // Checking variantAnnotation2
        variantAnnotation = outputVariantAnnotations.get(1);
        Assert.assertEquals(1, variantAnnotation.getTraitAssociation().size());
        Assert.assertEquals(HgmdVariantAnnotatorExtensionTask.ID, variantAnnotation.getTraitAssociation().get(0).getSource().getName());
        Assert.assertEquals(HGMD_VERSION, variantAnnotation.getTraitAssociation().get(0).getSource().getVersion());
        Assert.assertEquals(HGMD_ASSEMBLY, variantAnnotation.getTraitAssociation().get(0).getAssembly());
        Assert.assertEquals("CI144310", variantAnnotation.getTraitAssociation().get(0).getId());
        Assert.assertEquals("Methylenetetrahydrofolate_reductase_deficiency", variantAnnotation.getTraitAssociation().get(0).getHeritableTraits().get(0).getTrait());
    }

    @Test
    public void testSetupOverwriteHgmdVariantAnnotatorExtensionTask() throws Exception {
        Path outPath = getTempPath();
        if (!outPath.toFile().mkdirs()) {
            throw new IOException("Error creating the output path: " + outPath.toAbsolutePath());
        }
        System.out.println("outPath = " + outPath.toAbsolutePath());

        // Setup HGMD directory with initial version
        Path hgmdFile = initHgmdPath();
        System.out.println("hgmdFile = " + hgmdFile.toAbsolutePath());

        HgmdVariantAnnotatorExtensionTask task = new HgmdVariantAnnotatorExtensionTask(null);

        // First setup
        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams();
        params.setExtension(HgmdVariantAnnotatorExtensionTask.ID);
        params.setResources(Collections.singletonList(hgmdFile.toAbsolutePath().toString()));
        ObjectMap hgmdParams = new ObjectMap();
        hgmdParams.put(HGMD_VERSION_KEY, HGMD_VERSION);
        hgmdParams.put(HGMD_ASSEMBLY_KEY, HGMD_ASSEMBLY);
        params.setParams(hgmdParams);

        task.setup(params, null);

        ObjectMap firstMetadata = task.getMetadata();
        String firstCreationDate = firstMetadata.getString(INDEX_CREATION_DATE_KEY);
        Assert.assertEquals(HGMD_VERSION, firstMetadata.get(HGMD_VERSION_KEY));
        Assert.assertEquals(true, task.isAvailable());

        // Wait a bit to ensure different timestamp
        Thread.sleep(1000);

        // Second setup with overwrite (same version)
        params.setOverwrite(true);
        HgmdVariantAnnotatorExtensionTask task2 = new HgmdVariantAnnotatorExtensionTask(null);
        task2.setup(params, null);

        ObjectMap secondMetadata = task2.getMetadata();
        String secondCreationDate = secondMetadata.getString(INDEX_CREATION_DATE_KEY);
        Assert.assertEquals(HGMD_VERSION, secondMetadata.get(HGMD_VERSION_KEY));
        Assert.assertEquals(HGMD_ASSEMBLY, secondMetadata.get(HGMD_ASSEMBLY_KEY));
        Assert.assertEquals(true, task2.isAvailable());

        // Verify the index was recreated (different creation dates)
        Assert.assertNotEquals("Creation dates should be different after overwrite",
                firstCreationDate, secondCreationDate);
    }

    @Test
    public void testCompatibilityWithPreviousSetup() throws Exception {
        Path outPath = getTempPath();
        if (!outPath.toFile().mkdirs()) {
            throw new IOException("Error creating the output path: " + outPath.toAbsolutePath());
        }
        System.out.println("outPath = " + outPath.toAbsolutePath());

        // Setup HGMD with first version
        Path hgmdFile = initHgmdPath();
        System.out.println("hgmdFile = " + hgmdFile.toAbsolutePath());

        HgmdVariantAnnotatorExtensionTask task1 = new HgmdVariantAnnotatorExtensionTask(null);

        VariantAnnotationExtensionConfigureParams params1 = new VariantAnnotationExtensionConfigureParams();
        params1.setExtension(HgmdVariantAnnotatorExtensionTask.ID);
        params1.setResources(Collections.singletonList(hgmdFile.toAbsolutePath().toString()));
        ObjectMap hgmdParams1 = new ObjectMap();
        hgmdParams1.put(HGMD_VERSION_KEY, HGMD_VERSION);
        hgmdParams1.put(HGMD_ASSEMBLY_KEY, HGMD_ASSEMBLY);
        params1.setParams(hgmdParams1);

        task1.setup(params1, null);
        ObjectMap options1 = task1.getOptions();

        // Verify first setup is available
        Assert.assertEquals(true, task1.isAvailable());

        // Annotate some variants with first setup
        task1.pre();
        List<VariantAnnotation> inputVariantAnnotations = new ArrayList<>();

        Variant variant = new Variant("1", 930215, 930215, "A", "G");
        List<Variant> normalizedVariants = normalizeVariant(variant);
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setChromosome(normalizedVariants.get(0).getChromosome());
        variantAnnotation.setStart(normalizedVariants.get(0).getStart());
        variantAnnotation.setEnd(normalizedVariants.get(0).getEnd());
        variantAnnotation.setReference(normalizedVariants.get(0).getReference());
        variantAnnotation.setAlternate(normalizedVariants.get(0).getAlternate());
        inputVariantAnnotations.add(variantAnnotation);

        List<VariantAnnotation> outputVariantAnnotations1 = task1.apply(inputVariantAnnotations);
        task1.post();

        // Verify annotations from first setup
        Assert.assertEquals(1, outputVariantAnnotations1.size());
        Assert.assertTrue(CollectionUtils.isNotEmpty(outputVariantAnnotations1.get(0).getTraitAssociation()));

        // Use factory to load existing setup
        ObjectMap factoryOptions = new ObjectMap();
        factoryOptions.put(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(),
                HgmdVariantAnnotatorExtensionTask.ID);
        factoryOptions.putAll(options1);

        // Create new task using existing index (compatibility check)
        HgmdVariantAnnotatorExtensionTask task2 = (HgmdVariantAnnotatorExtensionTask) new VariantAnnotatorExtensionsFactory()
                .getVariantAnnotatorExtensions(factoryOptions).get(0);

        task2.setup(params1, null);
        task2.getOptions();

        // Verify task2 can read the same index
        Assert.assertEquals(true, task2.isAvailable());

        task2.pre();
        List<VariantAnnotation> outputVariantAnnotations2 = task2.apply(inputVariantAnnotations);
        task2.post();

        // Verify both tasks produce the same annotations
        Assert.assertEquals(outputVariantAnnotations1.size(), outputVariantAnnotations2.size());

        VariantAnnotation va1 = outputVariantAnnotations1.get(0);
        VariantAnnotation va2 = outputVariantAnnotations2.get(0);

        Assert.assertEquals(va1.getTraitAssociation().size(), va2.getTraitAssociation().size());
        Assert.assertEquals(va1.getTraitAssociation().get(0).getId(),
                va2.getTraitAssociation().get(0).getId());
        Assert.assertEquals(va1.getTraitAssociation().get(0).getSource().getName(),
                va2.getTraitAssociation().get(0).getSource().getName());
        Assert.assertEquals(va1.getTraitAssociation().get(0).getSource().getVersion(),
                va2.getTraitAssociation().get(0).getSource().getVersion());
    }

    @Test
    public void testIncompatibilityWithPreviousSetup() throws Exception {
        Path outPath = getTempPath();
        if (!outPath.toFile().mkdirs()) {
            throw new IOException("Error creating the output path: " + outPath.toAbsolutePath());
        }
        System.out.println("outPath = " + outPath.toAbsolutePath());

        // Setup HGMD with first version
        Path hgmdFile = initHgmdPath();
        System.out.println("hgmdFile = " + hgmdFile.toAbsolutePath());

        HgmdVariantAnnotatorExtensionTask task1 = new HgmdVariantAnnotatorExtensionTask(null);

        VariantAnnotationExtensionConfigureParams params1 = new VariantAnnotationExtensionConfigureParams();
        params1.setExtension(HgmdVariantAnnotatorExtensionTask.ID);
        params1.setResources(Collections.singletonList(hgmdFile.toAbsolutePath().toString()));
        ObjectMap hgmdParams1 = new ObjectMap();
        hgmdParams1.put(HGMD_VERSION_KEY, HGMD_VERSION);
        hgmdParams1.put(HGMD_ASSEMBLY_KEY, HGMD_ASSEMBLY);
        params1.setParams(hgmdParams1);

        task1.setup(params1, null);
        ObjectMap options1 = task1.getOptions();

        // Verify first setup is available
        Assert.assertEquals(true, task1.isAvailable());

        // Annotate some variants with first setup
        task1.pre();
        List<VariantAnnotation> inputVariantAnnotations = new ArrayList<>();

        Variant variant = new Variant("1", 930215, 930215, "A", "G");
        List<Variant> normalizedVariants = normalizeVariant(variant);
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setChromosome(normalizedVariants.get(0).getChromosome());
        variantAnnotation.setStart(normalizedVariants.get(0).getStart());
        variantAnnotation.setEnd(normalizedVariants.get(0).getEnd());
        variantAnnotation.setReference(normalizedVariants.get(0).getReference());
        variantAnnotation.setAlternate(normalizedVariants.get(0).getAlternate());
        inputVariantAnnotations.add(variantAnnotation);

        List<VariantAnnotation> outputVariantAnnotations1 = task1.apply(inputVariantAnnotations);
        task1.post();

        // Verify annotations from first setup
        Assert.assertEquals(1, outputVariantAnnotations1.size());
        Assert.assertTrue(CollectionUtils.isNotEmpty(outputVariantAnnotations1.get(0).getTraitAssociation()));

        // Use factory to load existing setup
        ObjectMap factoryOptions = new ObjectMap();
        factoryOptions.put(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(),
                HgmdVariantAnnotatorExtensionTask.ID);
        factoryOptions.putAll(options1);

        // Create new task using existing index (compatibility check)
        HgmdVariantAnnotatorExtensionTask task2 = (HgmdVariantAnnotatorExtensionTask) new VariantAnnotatorExtensionsFactory()
                .getVariantAnnotatorExtensions(factoryOptions).get(0);

        params1.getParams().put(HGMD_VERSION_KEY, "2025.3");
        ToolException exception = Assert.assertThrows(ToolException.class, () -> task2.setup(params1, null));
        Assert.assertTrue(exception.getMessage().contains("HGMD version"));
        Assert.assertTrue(exception.getMessage().contains("does not match"));


    }

    private static Path initHgmdPath() throws IOException {
        return initHgmdPath(getTempPath());
    }

    public static Path initHgmdPath(Path hgmdPath) throws IOException {
        if (!Files.isDirectory(hgmdPath) && !hgmdPath.toFile().mkdirs()) {
            throw new IOException("Error creating the HGMD path: " + hgmdPath.toAbsolutePath());
        }

        String cosmicFilename = "small_hgmd_pro_2020.3_hg38.vcf";
        Path targetPath = Paths.get(VariantStorageBaseTest.getResourceUri("custom_annotation/" + cosmicFilename, hgmdPath));

        if (!Files.exists(targetPath)) {
            throw new IOException("Error copying HGMD file to " + targetPath);
        }

        return targetPath;
    }

    private static Path getTempPath() {
        return Paths.get("target/test-data").resolve(TimeUtils.getTimeMillis() + "_" + RandomStringUtils.random(8, true, false));
    }

    private List<Variant> normalizeVariant(Variant variant) throws NonStandardCompliantSampleField {
        VariantNormalizer variantNormalizer = new VariantNormalizer(new VariantNormalizer.VariantNormalizerConfig()
                .setReuseVariants(true)
                .setNormalizeAlleles(true)
                .setDecomposeMNVs(false));

        return variantNormalizer.normalize(Collections.singletonList(variant), true);
    }
}