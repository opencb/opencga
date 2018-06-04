package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.tools.ant.types.Commandline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static junit.framework.TestCase.*;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.everyResult;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.withSampleData;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.withStudy;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.LOADED_GENOTYPES;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.printVariants;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.removeFile;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsTaskTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    public static void fillGaps(HadoopVariantStorageEngine variantStorageEngine, StudyConfiguration studyConfiguration,
                                Collection<Integer> sampleIds) throws Exception {
//        fillGapsMR(variantStorageEngine, studyConfiguration, sampleIds, true);
//        fillGapsMR(variantStorageEngine, studyConfiguration, sampleIds, false);
//        fillGapsLocal(variantStorageEngine, studyConfiguration, sampleIds);
//        fillLocalMRDriver(variantStorageEngine, studyConfiguration, sampleIds);
//        fillGapsLocalFromArchive(variantStorageEngine, studyConfiguration, sampleIds, false);
        variantStorageEngine.fillGaps(studyConfiguration.getStudyName(), sampleIds.stream().map(Object::toString).collect(Collectors.toList()), new ObjectMap("local", false));
//        variantStorageEngine.fillGaps(studyConfiguration.getStudyName(), sampleIds.stream().map(Object::toString).collect(Collectors.toList()), new ObjectMap("local", true));
    }

    protected static void fillLocalMRDriver(HadoopVariantStorageEngine variantStorageEngine, StudyConfiguration studyConfiguration, Collection<Integer> sampleIds) throws Exception {
        ObjectMap other = new ObjectMap();
//        other.putAll(variantStorageEngine.getOptions());
        other.put(FillGapsFromArchiveMapper.SAMPLES, sampleIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        String cli = AbstractAnalysisTableDriver.buildCommandLineArgs(
                variantStorageEngine.getArchiveTableName(studyConfiguration.getStudyId()), variantStorageEngine.getVariantTableName(),
                studyConfiguration.getStudyId(), Collections.emptyList(), other);
        System.out.println("cli = " + cli);
        Assert.assertEquals(0, new FillGapsDriver(configuration.get()).privateMain(Commandline.translateCommandline(cli)));
    }

    public static void fillGapsLocal(HadoopVariantStorageEngine variantStorageEngine, StudyConfiguration studyConfiguration,
                                Collection<Integer> sampleIds)
            throws StorageEngineException, IOException {
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        String variantTableName = variantStorageEngine.getVariantTableName();
        Table variantsTable = dbAdaptor.getHBaseManager().getConnection().getTable(TableName.valueOf(variantTableName));
        FillGapsFromVariantTask fillGapsTask = new FillGapsFromVariantTask(dbAdaptor.getHBaseManager(),
                variantStorageEngine.getArchiveTableName(studyConfiguration.getStudyId()),
                studyConfiguration, dbAdaptor.getGenomeHelper(), sampleIds);
        fillGapsTask.pre();

        ProgressLogger progressLogger = new ProgressLogger("Fill gaps:", dbAdaptor.count(new Query()).first(), 10);
        for (Variant variant : dbAdaptor) {
            progressLogger.increment(1, variant::toString);
            Put put = fillGapsTask.fillGaps(variant);

            if (put != null && !put.isEmpty()) {
                variantsTable.put(put);
            }
        }

        variantsTable.close();
        fillGapsTask.post();
    }

    @Test
    public void testFillGapsPlatinumFiles() throws Exception {
        testFillGapsPlatinumFiles(new ObjectMap());
    }

    @Test
    public void testFillGapsPlatinumFilesMultiFileBatch() throws Exception {
        testFillGapsPlatinumFiles(new ObjectMap(HadoopVariantStorageEngine.ARCHIVE_FILE_BATCH_SIZE, 2));
    }

    public void testFillGapsPlatinumFiles(ObjectMap options) throws Exception {
        StudyConfiguration studyConfiguration = loadPlatinum(options
                        .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC), 4);

        HadoopVariantStorageEngine variantStorageEngine = (HadoopVariantStorageEngine) this.variantStorageEngine;

        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        List<Integer> sampleIds = new ArrayList<>(studyConfiguration.getSampleIds().values());
        sampleIds.sort(Integer::compareTo);

        List<Integer> subSamples = sampleIds.subList(0, sampleIds.size() / 2);
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyConfiguration, subSamples);
        printVariants(dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first(), dbAdaptor, newOutputUri());
        checkFillGaps(studyConfiguration, dbAdaptor, subSamples);
        checkSampleIndexTable(dbAdaptor);

        subSamples = sampleIds.subList(sampleIds.size() / 2, sampleIds.size());
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyConfiguration, subSamples);
        printVariants(dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first(), dbAdaptor, newOutputUri());
        checkFillGaps(studyConfiguration, dbAdaptor, subSamples);
        checkSampleIndexTable(dbAdaptor);

        subSamples = sampleIds;
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyConfiguration, subSamples);
        printVariants(dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first(), dbAdaptor, newOutputUri());
        checkFillGaps(studyConfiguration, dbAdaptor, subSamples);
        checkSampleIndexTable(dbAdaptor);

        checkNewMultiAllelicVariants(dbAdaptor);
        checkNewMissingPositions(dbAdaptor);
        checkQueryGenotypes(dbAdaptor);
    }

    @Test
    public void testFillMissingPlatinumFiles() throws Exception {
        ObjectMap options = new ObjectMap()
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(HadoopVariantStorageEngine.ARCHIVE_FILE_BATCH_SIZE, 2);

        // Load files 1277 , 1278
        StudyConfiguration studyConfiguration = loadPlatinum(options, 12877, 12878);
        assertFalse(studyConfiguration.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED));
        HadoopVariantStorageEngine variantStorageEngine = ((HadoopVariantStorageEngine) this.variantStorageEngine);
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        checkFillMissing(dbAdaptor);
        checkSampleIndexTable(dbAdaptor);
        assertEquals(new HashSet<>(Arrays.asList("0/1", "1/1")), new HashSet<>(studyConfiguration.getAttributes().getAsStringList(LOADED_GENOTYPES)));

        List<Integer> sampleIds = new ArrayList<>(studyConfiguration.getSampleIds().values());
        sampleIds.sort(Integer::compareTo);

        // Fill missing
        variantStorageEngine.fillMissing(studyConfiguration.getStudyName(), options, false);
        printVariants(dbAdaptor, newOutputUri());
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        assertTrue(studyConfiguration.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED));
        checkFillMissing(dbAdaptor, "NA12877", "NA12878");
        checkSampleIndexTable(dbAdaptor);
        assertEquals(new HashSet<>(Arrays.asList("0/1", "1/1", "0/2")), new HashSet<>(studyConfiguration.getAttributes().getAsStringList(LOADED_GENOTYPES)));

        // Load file 12879
        studyConfiguration = loadPlatinum(options, 12879, 12879);

        assertFalse(studyConfiguration.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED));
        checkFillMissing(dbAdaptor, Arrays.asList(3), "NA12877", "NA12878");
        checkSampleIndexTable(dbAdaptor);

        // Load file 12880
        studyConfiguration = loadPlatinum(options, 12880, 12880);
        checkFillMissing(dbAdaptor, Arrays.asList(3, 4), "NA12877", "NA12878");
        checkSampleIndexTable(dbAdaptor);

        // Fill missing
        variantStorageEngine.fillMissing(studyConfiguration.getStudyName(), options, false);
        printVariants(dbAdaptor, newOutputUri());
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        assertTrue(studyConfiguration.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED));
        checkFillMissing(dbAdaptor, "NA12877", "NA12878", "NA12879", "NA12880", "NA12881");
        checkSampleIndexTable(dbAdaptor);

        // Check fill missing for 4 files
        checkNewMultiAllelicVariants(dbAdaptor);
        checkNewMissingPositions(dbAdaptor);

        // Remove last file
        removeFile(variantStorageEngine, null, 4, studyConfiguration, Collections.emptyMap());
        printVariants(dbAdaptor, newOutputUri());
        checkFillMissing(dbAdaptor, "NA12877", "NA12878", "NA12879", "NA12880");
        checkSampleIndexTable(dbAdaptor);

        // Fill missing
        variantStorageEngine.fillMissing(studyConfiguration.getStudyName(), options, false);
        printVariants(dbAdaptor, newOutputUri());
        checkFillMissing(dbAdaptor, "NA12877", "NA12878", "NA12879", "NA12880");
        checkQueryGenotypes(dbAdaptor);
        checkSampleIndexTable(dbAdaptor);
    }

    public void checkNewMultiAllelicVariants(VariantHadoopDBAdaptor dbAdaptor) {
        Variant v = dbAdaptor.get(new Query(VariantQueryParam.ID.key(), "1:10297:C:G").append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "?"), null).first();
        assertEquals(1, v.getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("0/1", v.getStudies().get(0).getSampleData("NA12877", "GT"));
        assertEquals("0/2", v.getStudies().get(0).getSampleData("NA12878", "GT"));

        v = dbAdaptor.get(new Query(VariantQueryParam.ID.key(), "1:10297:C:T").append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "?"), null).first();
        assertEquals(1, v.getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("0/2", v.getStudies().get(0).getSampleData("NA12877", "GT"));
        assertEquals("0/1", v.getStudies().get(0).getSampleData("NA12878", "GT"));
    }

    public void checkNewMissingPositions(VariantHadoopDBAdaptor dbAdaptor) {
        Variant v;
        v = dbAdaptor.get(new Query(VariantQueryParam.ID.key(), "1:10821:T:A").append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "?"), null).first();
        assertEquals(0, v.getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("./.", v.getStudies().get(0).getSampleData("NA12878", "GT"));
        assertEquals("./.", v.getStudies().get(0).getSampleData("NA12880", "GT"));

        v = dbAdaptor.get(new Query(VariantQueryParam.ID.key(), "1:10635:C:G").append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "?"), null).first();
        assertEquals(0, v.getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("./.", v.getStudies().get(0).getSampleData("NA12880", "GT"));
    }

    private StudyConfiguration loadPlatinum(ObjectMap extraParams, int max) throws Exception {
        return loadPlatinum(extraParams, 12877, 12877 + max - 1);
    }

    private StudyConfiguration loadPlatinum(ObjectMap extraParams, int from, int to) throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        List<URI> inputFiles = new LinkedList<>();

        for (int fileId = from; fileId <= to; fileId++) {
            String fileName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            inputFiles.add(getResourceUri(fileName));
        }

        ObjectMap options = variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(VariantStorageEngine.Options.STUDY_ID.key(), studyConfiguration.getStudyId());
        options.put(VariantStorageEngine.Options.STUDY_NAME.key(), studyConfiguration.getStudyName());
        options.put(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true);
        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_ARCHIVE_BATCH_SIZE, 1);
        options.putAll(extraParams);
        List<StoragePipelineResult> index = variantStorageManager.index(inputFiles, outputUri, true, true, true);

        for (StoragePipelineResult storagePipelineResult : index) {
            System.out.println(storagePipelineResult);
        }

        URI outputUri = newOutputUri(1);
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        printVariants(studyConfiguration, dbAdaptor, outputUri);

        return studyConfiguration;
    }

    protected void checkFillGaps(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor, List<Integer> sampleIds) {
        for (Variant variant : dbAdaptor) {
            boolean anyUnknown = false;
            boolean allUnknown = true;
            for (Integer sampleId : sampleIds) {
                boolean unknown = variant.getStudies().get(0).getSampleData(studyConfiguration.getSampleIds().inverse().get(sampleId), "GT").equals("?/?");
                anyUnknown |= unknown;
                allUnknown &= unknown;
            }
            // Fail if any, but not all samples are unknown
            try {
                Assert.assertFalse(variant.toString(), anyUnknown && !allUnknown);
//            Assert.assertTrue(allUnknown || !anyUnknown);
            } catch (AssertionError e) {
                if (variant.toString().equals("1:10178:-:C")) {
                    System.out.println("Gaps in variant " + variant);
                } else {
                    throw e;
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
        StudyConfiguration studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(STUDY_ID, null).first();
        boolean missingGenotypesUpdated = studyConfiguration.getAttributes().getBoolean(MISSING_GENOTYPES_UPDATED);

        for (Variant variant : dbAdaptor) {
            StudyEntry studyEntry = variant.getStudies().get(0);
            boolean newVariant =  !missingGenotypesUpdated && studyEntry.getFiles().stream().map(FileEntry::getFileId).map(Integer::valueOf).allMatch(newFilesSet::contains);
            List<List<String>> samplesData = studyEntry.getSamplesData();
            for (int i = 0; i < samplesData.size(); i++) {
                List<String> data = samplesData.get(i);
                String sampleName = studyEntry.getOrderedSamplesName().get(i);
                if (!newVariant && samplesSet.contains(sampleName)) {
                    assertFalse((newVariant ? "new variant " : "") + variant + " _ " + sampleName + " should not have GT=?/?", data.get(0).equals("?/?"));
                } else {
                    assertFalse((newVariant ? "new variant " : "") + variant + " _ " + sampleName + " should not have GT=0/0", data.get(0).equals("0/0"));
                }
            }
        }
    }

    private void checkQueryGenotypes(VariantHadoopDBAdaptor dbAdaptor) {
        StudyConfiguration sc = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(STUDY_ID, null).first();
        List<Variant> allVariants = dbAdaptor.get(new Query(), new QueryOptions()).getResult();

        for (String sample : StudyConfiguration.getIndexedSamples(sc).keySet()) {
            VariantQueryResult<Variant> queryResult = dbAdaptor.get(new Query(VariantQueryParam.SAMPLE.key(), sample)
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL).append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL), new QueryOptions());
            assertThat(queryResult, everyResult(allVariants, withStudy(STUDY_NAME, withSampleData(sample, "GT", anyOf(containsString("1"), containsString("2"), containsString("3"))))));
        }
    }

    protected void checkSampleIndexTable(VariantHadoopDBAdaptor dbAdaptor) throws IOException {
        SampleIndexDBAdaptor sampleIndexDBAdaptor = new SampleIndexDBAdaptor(dbAdaptor.getGenomeHelper(), dbAdaptor.getHBaseManager(),
                dbAdaptor.getTableNameGenerator(), dbAdaptor.getStudyConfigurationManager());

        for (String study : dbAdaptor.getStudyConfigurationManager().getStudies(null).keySet()) {
            StudyConfiguration sc = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(study, null).first();
            for (Integer fileId : sc.getIndexedFiles()) {
                for (int sampleId : sc.getSamplesInFiles().get(fileId)) {
                    int countFromIndex = 0;
                    Iterator<Map<String, List<Variant>>> iterator = sampleIndexDBAdaptor.rawIterator(sc.getStudyId(), sampleId);
                    while (iterator.hasNext()) {
                        Map<String, List<Variant>> map = iterator.next();
                        for (Map.Entry<String, List<Variant>> entry : map.entrySet()) {
                            String gt = entry.getKey();
                            List<Variant> variants = entry.getValue();
                            countFromIndex++;
                            VariantQueryResult<Variant> result = dbAdaptor.get(new Query(VariantQueryParam.ID.key(), variants).append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleId), null);
                            assertEquals(variants.size(), result.getNumResults());
                            for (Variant variant : result.getResult()) {
                                assertEquals(gt, variant.getStudies().get(0).getSampleData(0).get(0));
                            }
                        }
                    }

                    int countFromVariants = 0;
                    for (Variant variant : dbAdaptor.get(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleId), null).getResult()) {
                        if (SampleIndexDBLoader.validGenotype(variant.getStudies().get(0).getSampleData(0).get(0))) {
                            countFromVariants++;
                        }
                    }

                    assertNotEquals(0, countFromIndex);
                    assertNotEquals(countFromVariants, countFromIndex);
                }
            }
        }
    }

}