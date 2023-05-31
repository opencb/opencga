package org.opencb.opencga.storage.hadoop.variant.gaps;

import com.google.common.collect.BiMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateParams;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import static org.opencb.opencga.core.api.ParamConstants.ALL;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.ARCHIVE_NON_REF_FILTER;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.printVariants;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.removeFile;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(LongTests.class)
public class FillGapsTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    public static void fillGaps(HadoopVariantStorageEngine variantStorageEngine, StudyMetadata studyMetadata,
                                Collection<String> sampleIds) throws Exception {
//        fillGapsMR(variantStorageEngine, studyMetadata, sampleIds, true);
//        fillGapsMR(variantStorageEngine, studyMetadata, sampleIds, false);
//        fillGapsLocal(variantStorageEngine, studyMetadata, sampleIds);
//        fillLocalMRDriver(variantStorageEngine, studyMetadata, sampleIds);
//        fillGapsLocalFromArchive(variantStorageEngine, studyMetadata, sampleIds, false);
        variantStorageEngine.aggregateFamily(studyMetadata.getName(), new VariantAggregateFamilyParams(new ArrayList<>(sampleIds), false), new ObjectMap("local", false));
        //        variantStorageEngine.fillGaps(studyMetadata.getStudyName(), sampleIds.stream().map(Object::toString).collect(Collectors.toList()), new ObjectMap("local", true));
    }

    @Test
    public void testFillGapsVcfFiles() throws Exception {
        List<URI> inputFiles = new LinkedList<>();

        for (int fileId = 12877; fileId <= 12880; fileId++) {
            String fileName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.vcf.gz";
            inputFiles.add(getResourceUri(fileName));
        }

        StudyMetadata studyMetadata = load(new ObjectMap(VariantStorageOptions.GVCF.key(), false), inputFiles, newOutputUri(1));

        // Do not check new missing genotypes, as the input files does not have missing genotype regions
        testFillGapsPlatinumFiles(studyMetadata, false);
    }

    @Test
    public void testFillGapsPlatinumFiles() throws Exception {
        StudyMetadata studyMetadata = loadPlatinum(new ObjectMap()
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC), 12877, 12880);
        testFillGapsPlatinumFiles(studyMetadata, true);
    }

    @Test
    public void testFillGapsPlatinumFilesMultiFileBatch() throws Exception {
        StudyMetadata studyMetadata = loadPlatinum(new ObjectMap(HadoopVariantStorageOptions.ARCHIVE_FILE_BATCH_SIZE.key(), 2)
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC), 12877, 12880);
        testFillGapsPlatinumFiles(studyMetadata, true);
    }

    public void testFillGapsPlatinumFiles(StudyMetadata studyMetadata, boolean checkNewMissingGenotypes) throws Exception {

        HadoopVariantStorageEngine variantStorageEngine = (HadoopVariantStorageEngine) this.variantStorageEngine;

        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        BiMap<Integer, String> samplesMap = metadataManager.getIndexedSamplesMap(studyMetadata.getId()).inverse();
        List<Integer> sampleIds = new ArrayList<>(samplesMap.keySet());
        sampleIds.sort(Integer::compareTo);
        List<String> samples = sampleIds.stream().map(samplesMap::get).collect(Collectors.toList());

        List<String> subSamples = samples.subList(0, samples.size() / 2);
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyMetadata, subSamples);
        printVariants(studyMetadata, dbAdaptor, newOutputUri());
        checkFillGaps(studyMetadata, dbAdaptor, subSamples);
        checkSampleIndexTable(dbAdaptor);

        subSamples = samples.subList(samples.size() / 2, samples.size());
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyMetadata, subSamples);
        printVariants(studyMetadata, dbAdaptor, newOutputUri());
        checkFillGaps(studyMetadata, dbAdaptor, subSamples);
        checkSampleIndexTable(dbAdaptor);

        subSamples = samples;
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyMetadata, subSamples);
        printVariants(studyMetadata, dbAdaptor, newOutputUri());
        checkFillGaps(studyMetadata, dbAdaptor, subSamples);
        checkSampleIndexTable(dbAdaptor);

        checkNewMultiAllelicVariants(dbAdaptor);
        if (checkNewMissingGenotypes) {
            checkNewMissingPositions(dbAdaptor);
        }
        checkQueryGenotypes(dbAdaptor);
    }

    @Test
    public void testFillGapsConflictingFiles() throws Exception {
        StudyMetadata studyMetadata = load(new QueryOptions(), Arrays.asList(
                getResourceUri("gaps/file1.genome.vcf"),
                getResourceUri("gaps/file2.genome.vcf")));

        checkConflictingFiles(studyMetadata);
    }

    @Test
    public void testFillMissingFilterNonRef() throws Exception {
        StudyMetadata studyMetadata = load(new QueryOptions(VariantStorageOptions.GVCF.key(), true)
                .append(ARCHIVE_NON_REF_FILTER.key(), "FORMAT:DP<6"), Arrays.asList(
                getResourceUri("gaps/file1.genome.vcf"),
                getResourceUri("gaps/file2.genome.vcf")));

        variantStorageEngine.aggregate(studyMetadata.getName(), new VariantAggregateParams(true, false), new ObjectMap());
        VariantHadoopDBAdaptor dbAdaptor = (VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor();
        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        for (String file : Arrays.asList("file1.genome.vcf", "file2.genome.vcf")) {
            AtomicInteger refVariants = new AtomicInteger();
            System.out.println("Query archive file " + file);
            dbAdaptor.iterator(new VariantQuery().study(studyMetadata.getName()).file(file),
                    new QueryOptions("archive", true)
                            .append("ref", false)).forEachRemaining(variant -> {
                System.out.println("variant = " + variant);
                if (variant.getType().equals(VariantType.NO_VARIATION)) {
                    StudyEntry study = variant.getStudies().get(0);
                    Integer dpIdx = study.getSampleDataKeyPosition("DP");
                    if (dpIdx != null) {
                        String dpStr = study.getSampleData(0).get(dpIdx);
                        try {
                            Integer dp = Integer.valueOf(dpStr);
                            assertTrue(dp <= 5);
                            refVariants.getAndIncrement();
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            });
            assertTrue(refVariants.get() > 0);
        }

        Variant variant = dbAdaptor.get(new VariantQuery().id("1:10032:A:G").includeSample(ALL), null).first();
        assertEquals("0/0", variant.getStudies().get(0).getSampleData("s1", "GT"));
        assertEquals("5", variant.getStudies().get(0).getSampleData("s1", "DP"));
        variant = dbAdaptor.get(new VariantQuery().id("1:10050:A:T").includeSample(ALL), null).first();
        assertEquals("0/0", variant.getStudies().get(0).getSampleData("s2", "GT"));
        assertEquals("other", variant.getStudies().get(0).getSampleData("s2", "OTHER"));
    }

    @Test
    public void testFillGapsConflictingFilesNonRef() throws Exception {
        StudyMetadata studyMetadata = load(new QueryOptions(), Arrays.asList(
                getResourceUri("gaps2/file1.genome.vcf"),
                getResourceUri("gaps2/file2.genome.vcf")));

        checkConflictingFiles(studyMetadata);

        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        Variant variantMulti = dbAdaptor.get(new VariantQuery().id("1:10035:A:G").includeSample(ALL), null).first();
        assertEquals("0/0", variantMulti.getStudies().get(0).getSampleData("s1", "GT"));
        assertEquals(new AlternateCoordinate("1", 10035, 10035, "A", "<*>", VariantType.NO_VARIATION),
                variantMulti.getStudies().get(0).getSecondaryAlternates().get(0));
        assertEquals("4,0,1", variantMulti.getStudies().get(0).getSampleData("s1", "AD"));
        assertEquals("0/1", variantMulti.getStudies().get(0).getSampleData("s2", "GT"));
        assertEquals("13,23,0", variantMulti.getStudies().get(0).getSampleData("s2", "AD"));
    }

    public void checkConflictingFiles(StudyMetadata studyMetadata) throws Exception {
        HadoopVariantStorageEngine variantStorageEngine = (HadoopVariantStorageEngine) this.variantStorageEngine;

        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        BiMap<Integer, String> samplesMap = metadataManager.getIndexedSamplesMap(studyMetadata.getId()).inverse();
        List<Integer> sampleIds = new ArrayList<>(samplesMap.keySet());
        sampleIds.sort(Integer::compareTo);
        List<String> samples = sampleIds.stream().map(samplesMap::get).collect(Collectors.toList());

        fillGaps(variantStorageEngine, studyMetadata, samples);
        printVariants(studyMetadata, dbAdaptor, newOutputUri(1));
        checkFillGaps(studyMetadata, dbAdaptor, samples, Collections.singleton("1:10020:A:T"));
        checkSampleIndexTable(dbAdaptor);

        // Not a gap anymore since #1367
        Variant variantGap = dbAdaptor.get(new VariantQuery().id("1:10020:A:T").includeSample(ALL), null).first();
        assertEquals("0/1", variantGap.getStudies().get(0).getSampleData("s1", "GT"));
        assertEquals("./.", variantGap.getStudies().get(0).getSampleData("s2", "GT"));
        // Call generated by VariantReferenceBlockCreatorTask
        assertEquals("1:10015-10030:N:.", variantGap.getStudies().get(0).getFile("file2.genome.vcf").getCall().getVariantId());

        Variant variantMulti = dbAdaptor.get(new VariantQuery().id("1:10012:TTT:-").includeSample(ALL), null).first();
        assertEquals("<*>", variantMulti.getStudies().get(0).getSecondaryAlternates().get(0).getAlternate());
        assertEquals("0/1", variantMulti.getStudies().get(0).getSampleData("s1", "GT"));
        assertEquals("2/2", variantMulti.getStudies().get(0).getSampleData("s2", "GT"));

        Variant variantNonMulti = dbAdaptor.get(new VariantQuery().id("1:10054:A:G").includeSample(ALL), null).first();
        assertEquals(new HashSet<>(Arrays.asList("C", "T")),
                variantNonMulti.getStudies().get(0).getSecondaryAlternates().stream().map(AlternateCoordinate::getAlternate).collect(Collectors.toSet()));
        assertEquals("2/3", variantNonMulti.getStudies().get(0).getSampleData("s1", "GT"));
        assertEquals("0/1", variantNonMulti.getStudies().get(0).getSampleData("s2", "GT"));

    }

    @Test
    public void testFillMissingPlatinumFiles() throws Exception {
        ObjectMap options = new ObjectMap()
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(HadoopVariantStorageOptions.ARCHIVE_FILE_BATCH_SIZE.key(), 2);

        // Load files 12877 , 12878
        StudyMetadata studyMetadata = loadPlatinum(options, 12877, 12878);
        assertFalse(studyMetadata.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED));
        HadoopVariantStorageEngine variantStorageEngine = ((HadoopVariantStorageEngine) this.variantStorageEngine);
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        checkFillMissing(dbAdaptor);
        checkSampleIndexTable(dbAdaptor);
        assertEquals(new HashSet<>(Arrays.asList("0/1", "1/1")), new HashSet<>(studyMetadata.getAttributes().getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key())));

        List<Integer> sampleIds = new ArrayList<>(metadataManager.getIndexedSamplesMap(studyMetadata.getId()).values());
        sampleIds.sort(Integer::compareTo);

        // Fill missing
        options.put(HadoopVariantStorageOptions.FILL_MISSING_SIMPLIFIED_MULTIALLELIC_VARIANTS.key(), true); // This is the default
        variantStorageEngine.aggregate(studyMetadata.getName(), new VariantAggregateParams(false, false), options);
        printVariants(dbAdaptor, newOutputUri());
        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        assertTrue(studyMetadata.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED));
        checkFillMissing(dbAdaptor, "NA12877", "NA12878");
        checkSampleIndexTable(dbAdaptor);
        assertEquals(new HashSet<>(Arrays.asList("0/1", "1/1", "0/2")), new HashSet<>(studyMetadata.getAttributes().getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key())));

        // Load file 12879
        studyMetadata = loadPlatinum(options, 12879, 12879);

        assertFalse(studyMetadata.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED));
        checkFillMissing(dbAdaptor, Arrays.asList(3), "NA12877", "NA12878");
        checkSampleIndexTable(dbAdaptor);

        // Load file 12880
        studyMetadata = loadPlatinum(options, 12880, 12880);
        checkFillMissing(dbAdaptor, Arrays.asList(3, 4), "NA12877", "NA12878");
        checkSampleIndexTable(dbAdaptor);

        // Fill missing
        variantStorageEngine.aggregate(studyMetadata.getName(), new VariantAggregateParams(false, false), options);
        printVariants(dbAdaptor, newOutputUri());
        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        assertTrue(studyMetadata.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED));
        checkFillMissing(dbAdaptor, "NA12877", "NA12878", "NA12879", "NA12880", "NA12881");
        checkSampleIndexTable(dbAdaptor);

        // Check fill missing for 4 files
        checkNewMultiAllelicVariants(dbAdaptor);
        checkNewMissingPositions(dbAdaptor);

        // Remove last file
        removeFile(variantStorageEngine, null, "1K.end.platinum-genomes-vcf-NA12880_S1.genome.vcf.gz", studyMetadata, Collections.emptyMap(), outputUri);
        printVariants(dbAdaptor, newOutputUri());
        checkFillMissing(dbAdaptor, "NA12877", "NA12878", "NA12879", "NA12880");
        checkSampleIndexTable(dbAdaptor);

        // Fill missing
        variantStorageEngine.aggregate(studyMetadata.getName(), new VariantAggregateParams(false, false), options);
        printVariants(dbAdaptor, newOutputUri());
        checkFillMissing(dbAdaptor, "NA12877", "NA12878", "NA12879", "NA12880");
        checkQueryGenotypes(dbAdaptor);
        checkSampleIndexTable(dbAdaptor);
    }

    @Test
    public void testFillGapsCorpasome() throws Exception {
        StudyMetadata study = load(new ObjectMap(), Collections.singletonList(getResourceUri("quartet.variants.annotated.partial.vcf.gz")));
        printVariants((VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor(), newOutputUri());

        variantStorageEngine.aggregateFamily(study.getName(), new VariantAggregateFamilyParams()
                .setSamples(Arrays.asList("ISDBM322015", "ISDBM322016", "ISDBM322017", "ISDBM322018")), new ObjectMap());
        printVariants((VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor(), newOutputUri());

    }

    @Test
    public void testFillGapsImpact() throws Exception {
        URI uri = newOutputUri();
        ObjectMap extraParams = new ObjectMap(VariantStorageOptions.LOAD_HOM_REF.key(), true);
//        extraParams.append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "proto");
//        extraParams.append(VariantStorageOptions.GVCF.key(), true);
        StudyMetadata study = load(extraParams, Collections.singletonList(getResourceUri("impact/HG005_GRCh38_1_22_v4.2.1_benchmark.tuned.chr6-31.vcf.gz")), uri, false);
        load(extraParams, Collections.singletonList(getResourceUri("impact/HG006_GRCh38_1_22_v4.2.1_benchmark.tuned.chr6-31.vcf.gz")), uri, false);
        load(extraParams, Collections.singletonList(getResourceUri("impact/HG007_GRCh38_1_22_v4.2.1_benchmark.tuned.chr6-31.vcf.gz")), uri, false);
        printVariants((VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor(), newOutputUri());

        variantStorageEngine.aggregateFamily(study.getName(), new VariantAggregateFamilyParams()
                .setSamples(Arrays.asList("HG005", "HG006", "HG007")), new ObjectMap());
        printVariants((VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor(), newOutputUri());

    }

    public void checkNewMultiAllelicVariants(VariantHadoopDBAdaptor dbAdaptor) {
        Variant v = dbAdaptor.get(new VariantQuery().id("1:10297:C:G").unknownGenotype("?").includeSample(ALL), null).first();
        assertEquals(1, v.getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("0/1", v.getStudies().get(0).getSampleData("NA12877", "GT"));
        assertEquals("0/2", v.getStudies().get(0).getSampleData("NA12878", "GT"));

        v = dbAdaptor.get(new VariantQuery().id("1:10297:C:T").unknownGenotype("?").includeSample(ALL), null).first();
        assertEquals(1, v.getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("0/2", v.getStudies().get(0).getSampleData("NA12877", "GT"));
        assertEquals("0/1", v.getStudies().get(0).getSampleData("NA12878", "GT"));
    }

    public void checkNewMissingPositions(VariantHadoopDBAdaptor dbAdaptor) {
        Variant v;
        v = dbAdaptor.get(new VariantQuery().id("1:10821:T:A").includeSample(ALL).unknownGenotype("?"), null).first();
        assertEquals(0, v.getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("./.", v.getStudies().get(0).getSampleData("NA12878", "GT"));
        assertEquals("./.", v.getStudies().get(0).getSampleData("NA12880", "GT"));

        v = dbAdaptor.get(new VariantQuery().id("1:10635:C:G").includeSample(ALL).unknownGenotype("?"), null).first();
        assertEquals(0, v.getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("./.", v.getStudies().get(0).getSampleData("NA12880", "GT"));
    }

    private StudyMetadata loadPlatinum(ObjectMap extraParams, int max) throws Exception {
        return loadPlatinum(extraParams, 12877, 12877 + max - 1);
    }

    private StudyMetadata loadPlatinum(ObjectMap extraParams, int from, int to) throws Exception {

        List<URI> inputFiles = new LinkedList<>();

        for (int fileId = from; fileId <= to; fileId++) {
            String fileName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            inputFiles.add(getResourceUri(fileName));
        }

        return load(extraParams, inputFiles, newOutputUri(1));
    }

    private StudyMetadata load(ObjectMap extraParams, List<URI> inputFiles) throws Exception {
        return load(extraParams, inputFiles, newOutputUri(1));
    }

    private StudyMetadata load(ObjectMap extraParams, List<URI> inputFiles, URI outputUri) throws Exception {
        return load(extraParams, inputFiles, outputUri, true);
    }

    private StudyMetadata load(ObjectMap extraParams, List<URI> inputFiles, URI outputUri, boolean printVariants) throws Exception {
        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        HadoopVariantStorageEngine engine = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = engine.getDBAdaptor();

        ObjectMap options = engine.getOptions();
        options.put(VariantStorageOptions.STUDY.key(), studyMetadata.getName());
        options.put(VariantStorageOptions.GVCF.key(), true);
        options.put(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true);
        options.put(HadoopVariantStorageOptions.HADOOP_LOAD_FILES_IN_PARALLEL.key(), 1);
        options.put(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        options.putAll(extraParams);
        List<StoragePipelineResult> index = engine.index(inputFiles, outputUri, true, true, true);

        for (StoragePipelineResult storagePipelineResult : index) {
            System.out.println(storagePipelineResult);
        }

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        if (printVariants) {
            printVariants(studyMetadata, dbAdaptor, outputUri);
        }

        return studyMetadata;
    }

    protected void checkFillGaps(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor, List<String> samples) {
        checkFillGaps(studyMetadata, dbAdaptor, samples, Collections.singleton("1:10178:-:C"));
    }

    protected void checkFillGaps(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor, List<String> samples, Set<String> variantsWithGaps) {
        for (Variant variant : dbAdaptor.iterable(new VariantQuery().includeSample(samples), new QueryOptions())) {
            boolean anyUnknown = false;
            boolean allUnknown = true;
            for (String sampleName : samples) {
                boolean unknown = variant.getStudies().get(0).getSampleData(sampleName, "GT").equals("?/?");
                anyUnknown |= unknown;
                allUnknown &= unknown;
            }
            // Fail if any, but not all samples are unknown
            if (anyUnknown && !allUnknown) {
                if (variantsWithGaps.contains(variant.toString())) {
                    System.out.println("Gaps in variant " + variant);
                } else {
                    Assert.fail("Gaps in variant " + variant);
                }
            }
        }
    }

    protected void checkFillMissing(VariantHadoopDBAdaptor dbAdaptor, String... processedSamples) {
        checkFillMissing(dbAdaptor, Arrays.asList(), processedSamples);
    }

    protected void checkFillMissing(VariantHadoopDBAdaptor dbAdaptor, List<Integer> newFiles, String... processedSamples) {
        Set<Integer> newFilesSet = new HashSet<>(newFiles);
        Set<String> samplesSet = new HashSet<>(Arrays.asList(processedSamples));
        StudyMetadata studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(STUDY_NAME);
        boolean missingGenotypesUpdated = studyMetadata.getAttributes().getBoolean(MISSING_GENOTYPES_UPDATED);

        for (Variant variant : dbAdaptor) {
            StudyEntry studyEntry = variant.getStudies().get(0);
            boolean newVariant = !missingGenotypesUpdated && studyEntry.getFiles().stream().map(FileEntry::getFileId)
                    .map(name -> metadataManager.getFileId(studyMetadata.getId(), name)).allMatch(newFilesSet::contains);
            List<SampleEntry> sampleEntries = studyEntry.getSamples();
            for (int i = 0; i < sampleEntries.size(); i++) {
                List<String> data = sampleEntries.get(i).getData();
                String sampleName = studyEntry.getOrderedSamplesName().get(i);
                if (newVariant || !samplesSet.contains(sampleName)) {
                    assertNotEquals((newVariant ? "new variant " : "") + variant + " _ " + sampleName + " should not have GT=0/0", "0/0", data.get(0));
                } else {
                    assertNotEquals(variant + " _ " + sampleName + " should not have GT=?/?", "?/?", data.get(0));
                }
            }
        }
    }

    private void checkQueryGenotypes(VariantHadoopDBAdaptor dbAdaptor) {
        StudyMetadata sc = dbAdaptor.getMetadataManager().getStudyMetadata(STUDY_NAME);
        List<Variant> allVariants = dbAdaptor.get(new VariantQuery().includeSample(ALL), new QueryOptions()).getResults();

        for (String sample : metadataManager.getIndexedSamplesMap(sc.getId()).keySet()) {
            VariantQueryResult<Variant> queryResult = dbAdaptor.get(new VariantQuery().genotype(sample + ":" + "0/1,0|1,1|0,1|1,1/1")
                    .includeSample(ALL), new QueryOptions("explain", true));
            System.out.println("queryResult.getResults() = " + queryResult.getResults());
            assertThat(queryResult, everyResult(allVariants,
                    withStudy(STUDY_NAME,
                            withSampleData(sample, "GT", containsString("1")))));
        }
    }

    protected void checkSampleIndexTable(VariantHadoopDBAdaptor dbAdaptor) throws IOException {
        SampleIndexDBAdaptor sampleIndexDBAdaptor = new SampleIndexDBAdaptor(dbAdaptor.getHBaseManager(),
                dbAdaptor.getTableNameGenerator(), dbAdaptor.getMetadataManager());

        for (String study : metadataManager.getStudies(null).keySet()) {
            StudyMetadata sc = metadataManager.getStudyMetadata(study);
            for (Integer fileId : metadataManager.getIndexedFiles(sc.getId())) {
                for (int sampleId : metadataManager.getFileMetadata(sc.getId(), fileId).getSamples()) {
                    String sample = metadataManager.getSampleName(sc.getId(), sampleId);
                    String message = "Sample '" + sample + "' : " + sampleId;
                    int countFromIndex = 0;
                    Iterator<Map<String, List<Variant>>> iterator = sampleIndexDBAdaptor.iteratorByGt(sc.getId(), sampleId);
                    while (iterator.hasNext()) {
                        Map<String, List<Variant>> map = iterator.next();
                        for (Map.Entry<String, List<Variant>> entry : map.entrySet()) {
                            String gt = entry.getKey();
                            List<Variant> variants = entry.getValue();
                            countFromIndex += variants.size();
                            VariantQueryResult<Variant> result = dbAdaptor.get(new VariantQuery()
                                    .id(variants)
                                    .study(study)
                                    .includeSample(sample), null);
                            Set<String> expected = variants.stream().map(Variant::toString).collect(Collectors.toSet());
                            Set<String> actual = result.getResults().stream().map(Variant::toString).collect(Collectors.toSet());
                            if (!expected.equals(actual)) {
                                HashSet<String> extra = new HashSet<>(actual);
                                extra.removeAll(expected);
                                HashSet<String> missing = new HashSet<>(expected);
                                missing.removeAll(actual);
                                System.out.println("missing = " + missing);
                                System.out.println("extra = " + extra);
                            }
                            assertEquals(message, variants.size(), result.getNumResults());
                            for (Variant variant : result.getResults()) {
                                assertEquals(message, gt, variant.getStudies().get(0).getSampleData(sample, "GT"));
                            }
                        }
                    }

                    int countFromVariants = 0;
                    for (Variant variant : dbAdaptor.get(new VariantQuery().includeSample(sample), null).getResults()) {
                        String gt = variant.getStudies().get(0).getSampleData(0).get(0);
                        if (!gt.equals(GenotypeClass.UNKNOWN_GENOTYPE) && SampleIndexSchema.validGenotype(gt)) {
                            countFromVariants++;
                        }
                    }

                    assertNotEquals(message, 0, countFromIndex);
                    assertEquals(message, countFromVariants, countFromIndex);
                }
            }
        }
    }

}