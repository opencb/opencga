/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageEngineTest extends VariantStorageBaseTest {

    private static Logger logger = LoggerFactory.getLogger(VariantStorageEngineTest.class);

    @Test
    public void basicIndex() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "json"));
        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.json.gz'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.json.gz"));
        VariantFileMetadata fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(etlResult.getTransformResult());
        assertEquals(1, studyConfiguration.getIndexedFiles().size());
        checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, true, false, true, getExpectedNumLoadedVariants(fileMetadata));
    }

    @Test
    public void avroBasicIndex() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro"));
        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.avro.gz'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.avro.gz"));

        assertEquals(1, studyConfiguration.getIndexedFiles().size());
        VariantFileMetadata fileMetadata = checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, true, false, true, getExpectedNumLoadedVariants
                (fileMetadata));
    }

    @Test
    public void multiIndex() throws Exception {
        clearDB(DB_NAME);
        int expectedNumVariants = NUM_VARIANTS - 37; //37 variants have been removed from this dataset because had the genotype 0|0 for
        // each sample
        StudyConfiguration studyConfigurationMultiFile = new StudyConfiguration(1, "multi");

        StoragePipelineResult etlResult;
        ObjectMap options = new ObjectMap()
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.CONTROL_SET)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);
        URI file1Uri = getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file1Uri, variantStorageEngine, studyConfigurationMultiFile, options);
        Integer defaultCohortId = studyConfigurationMultiFile.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
        assertTrue(studyConfigurationMultiFile.getCohorts().containsKey(defaultCohortId));
        assertEquals(500, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertEquals(Collections.emptySet(), studyConfigurationMultiFile.getCalculatedStats());
        assertEquals(Collections.emptySet(), studyConfigurationMultiFile.getInvalidStats());
        Integer fileId1 = studyConfigurationMultiFile.getFileIds().get(UriUtils.fileName(file1Uri));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId1));

        options.append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);
        URI file2Uri = getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file2Uri, variantStorageEngine, studyConfigurationMultiFile, options);
        assertEquals(1000, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertEquals(Collections.singleton(defaultCohortId), studyConfigurationMultiFile.getCalculatedStats());
        assertEquals(Collections.emptySet(), studyConfigurationMultiFile.getInvalidStats());
        Integer fileId2 = studyConfigurationMultiFile.getFileIds().get(UriUtils.fileName(file2Uri));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId2));

        options.append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);
        URI file3Uri = getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file3Uri, variantStorageEngine, studyConfigurationMultiFile, options);
        assertEquals(1500, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertEquals(Collections.emptySet(), studyConfigurationMultiFile.getCalculatedStats());
        assertEquals(Collections.singleton(defaultCohortId), studyConfigurationMultiFile.getInvalidStats());
        int fileId3 = studyConfigurationMultiFile.getFileIds().get(UriUtils.fileName(file3Uri));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId3));

        options.append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);
        URI file4Uri = getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file4Uri, variantStorageEngine, studyConfigurationMultiFile, options);
        assertEquals(2000, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        int fileId4 = studyConfigurationMultiFile.getFileIds().get(UriUtils.fileName(file4Uri));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId4));

        URI file5Uri = getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file5Uri, variantStorageEngine, studyConfigurationMultiFile, options);
        int fileId5 = studyConfigurationMultiFile.getFileIds().get(UriUtils.fileName(file5Uri));
        assertEquals(2504, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId1));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId2));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId3));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId4));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(fileId5));

        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        checkLoadedVariants(dbAdaptor, studyConfigurationMultiFile, true, false, false, expectedNumVariants);


        //Load, in a new study, the same dataset in one single file
        StudyConfiguration studyConfigurationSingleFile = new StudyConfiguration(2, "single");
        URI fileUri = getResourceUri("filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        etlResult = runDefaultETL(fileUri, variantStorageEngine, studyConfigurationSingleFile, options);
        int fileId = studyConfigurationSingleFile.getFileIds().get(UriUtils.fileName(fileUri));
        assertTrue(studyConfigurationSingleFile.getIndexedFiles().contains(fileId));

        checkTransformedVariants(etlResult.getTransformResult(), studyConfigurationSingleFile);


        //Check that both studies contains the same information
        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantQueryParam.STUDY.key(),
                studyConfigurationMultiFile.getStudyId() + "," + studyConfigurationSingleFile.getStudyId()), new QueryOptions());
        int numVariants = 0;
        for (; iterator.hasNext(); ) {
            Variant variant = iterator.next();
            numVariants++;
//            Map<String, VariantSourceEntry> map = variant.getStudies().stream().collect(Collectors.toMap
// (VariantSourceEntry::getStudyId, Function.<VariantSourceEntry>identity()));
            Map<String, StudyEntry> map = variant.getStudiesMap();

            assertTrue(variant.toString(), map.containsKey(studyConfigurationMultiFile.getStudyName()));
            assertTrue(variant.toString(), map.containsKey(studyConfigurationSingleFile.getStudyName()));
            String expected = map.get(studyConfigurationSingleFile.getStudyName()).getSamplesData().toString();
            String actual = map.get(studyConfigurationMultiFile.getStudyName()).getSamplesData().toString();
            if (!assertWithConflicts(variant, () -> assertEquals(variant.toString(), expected, actual))) {
                List<List<String>> samplesDataSingle = map.get(studyConfigurationSingleFile.getStudyName()).getSamplesData();
                List<List<String>> samplesDataMulti = map.get(studyConfigurationMultiFile.getStudyName()).getSamplesData();
                for (int i = 0; i < samplesDataSingle.size(); i++) {
                    String sampleName = map.get(studyConfigurationMultiFile.getStudyName()).getOrderedSamplesName().get(i);
                    String message = variant.toString()
                            + " sample: " + sampleName
                            + " id " + studyConfigurationMultiFile.getSampleIds().get(sampleName);
                    String expectedGt = samplesDataSingle.get(i).toString();
                    String actualGt = samplesDataMulti.get(i).toString();
                    assertWithConflicts(variant, () -> assertEquals(message, expectedGt, actualGt));
                }
            }
        }
        assertEquals(expectedNumVariants, numVariants);

    }


    @Test
    public void multiIndexPlatinum() throws Exception {
        multiIndexPlatinum(new ObjectMap());
    }

    public void multiIndexPlatinum(ObjectMap options) throws Exception {
        clearDB(DB_NAME);
        // each sample
        StudyConfiguration studyConfigurationMultiFile = new StudyConfiguration(1, "multi");
        StudyConfiguration studyConfigurationBatchFile = new StudyConfiguration(2, "batch");

        options.putIfAbsent(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.UNKNOWN);
        options.putIfAbsent(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);
        options.putIfAbsent(VariantStorageEngine.Options.ANNOTATE.key(), false);

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();
        StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
        int i = 1;
        for (int fileId = 77; fileId <= 93; fileId++) {
            ObjectMap fileOptions = new ObjectMap();
            fileOptions.putAll(options);
            runDefaultETL(getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA128" + fileId + "_S1.genome.vcf.gz"),
                    variantStorageManager, studyConfigurationMultiFile, fileOptions);
            studyConfigurationMultiFile = studyConfigurationManager.getStudyConfiguration(studyConfigurationMultiFile.getStudyId(), null).first();
            assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(i));
            i++;
        }


        List<URI> uris = new LinkedList<>();
        for (int fileId = 77; fileId <= 93; fileId++) {
            uris.add(getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA128" + fileId + "_S1.genome.vcf.gz"));
        }

        variantStorageManager = getVariantStorageEngine();
        variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions()
                .append(VariantStorageEngine.Options.STUDY.key(), studyConfigurationBatchFile.getStudyName())
                .putAll(options);

        List<StoragePipelineResult> results = variantStorageManager.index(uris, outputUri, true, true, true);

        for (StoragePipelineResult result : results) {
            System.out.println(result.toString());
            assertTrue(result.isTransformExecuted());
            assertNull(result.getTransformError());
            assertTrue(result.isLoadExecuted());
            assertNull(result.getLoadError());
        }

        studyConfigurationBatchFile = studyConfigurationManager.getStudyConfiguration(studyConfigurationBatchFile.getStudyId(), null).first();
        checkLoadedVariants(dbAdaptor, studyConfigurationBatchFile, true, false, -1);


        dbAdaptor.close();
        studyConfigurationManager.close();

//
//        //Load, in a new study, the same dataset in one single file
//        StudyConfiguration studyConfigurationSingleFile = new StudyConfiguration(2, "single");
//        etlResult = runDefaultETL(getResourceUri("filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
//                variantStorageManager, studyConfigurationSingleFile, options.append(VariantStorageEngine.Options.FILE_ID.key(), 10));
//        assertTrue(studyConfigurationSingleFile.getIndexedFiles().contains(10));
//
//        checkTransformedVariants(etlResult.getTransformResult(), studyConfigurationSingleFile);
//
//
//        //Check that both studies contains the same information
//        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(),
//                studyConfigurationMultiFile.getStudyId() + "," + studyConfigurationSingleFile.getStudyId()), new QueryOptions());
//        int numVariants = 0;
//        for (; iterator.hasNext(); ) {
//            Variant variant = iterator.next();
//            numVariants++;
////            Map<String, VariantSourceEntry> map = variant.getStudies().stream().collect(Collectors.toMap
//// (VariantSourceEntry::getStudyId, Function.<VariantSourceEntry>identity()));
//            Map<String, StudyEntry> map = variant.getStudiesMap();
//
//            assertTrue(map.containsKey(studyConfigurationMultiFile.getStudyName()));
//            assertTrue(map.containsKey(studyConfigurationSingleFile.getStudyName()));
//            assertEquals(map.get(studyConfigurationSingleFile.getStudyName()).getSamplesData(), map.get(studyConfigurationMultiFile
//                    .getStudyName()).getSamplesData());
//        }
//        assertEquals(expectedNumVariants - 4, numVariants);

    }

    @Test
    public void multiRegionBatchIndex() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = new StudyConfiguration(1, "multiRegion");

        StoragePipelineResult etlResult;
        ObjectMap options = new ObjectMap()
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.CONTROL_SET)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();

        URI chr1 = getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI chr22 = getResourceUri("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

//        runDefaultETL(chr1, this.variantStorageEngine,
//                studyConfiguration, options.append(VariantStorageEngine.Options.FILE_ID.key(), 5));
//        Integer defaultCohortId = studyConfiguration.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
//        assertTrue(studyConfiguration.getCohorts().containsKey(defaultCohortId));
//        assertEquals(2504, studyConfiguration.getCohorts().get(defaultCohortId).size());
//        assertTrue(studyConfiguration.getIndexedFiles().contains(5));
//        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, true, false, -1);
//
//        runDefaultETL(chr22, this.variantStorageEngine,
////        runDefaultETL(getResourceUri("1k.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"), variantStorageEngine,
//                studyConfiguration, options.append(VariantStorageEngine.Options.FILE_ID.key(), 6));

        variantStorageEngine.getOptions()
                .append(VariantStorageEngine.Options.STUDY.key(), STUDY_NAME)
                .append(VariantStorageEngine.Options.LOAD_SPLIT_DATA.key(), true);

        List<StoragePipelineResult> results = variantStorageEngine.index(Arrays.asList(chr1, chr22), outputUri, true, true, true);

        for (StoragePipelineResult result : results) {
            System.out.println(result.toString());
            assertTrue(result.isTransformExecuted());
            assertNull(result.getTransformError());
            assertTrue(result.isLoadExecuted());
            assertNull(result.getLoadError());
        }


    }

    @Test
    public void multiRegionIndex() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = new StudyConfiguration(1, "multiRegion");

        ObjectMap options = new ObjectMap()
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.CONTROL_SET)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);

        runDefaultETL(getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
                studyConfiguration, options);
        Integer defaultCohortId = studyConfiguration.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
        int fileIdChr1 = studyConfiguration.getFileIds().get("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        assertTrue(studyConfiguration.getCohorts().containsKey(defaultCohortId));
        assertEquals(2504, studyConfiguration.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfiguration.getIndexedFiles().contains(fileIdChr1));
        checkLoadedVariants(getVariantStorageEngine().getDBAdaptor(), studyConfiguration, true, false, false, -1);

        runDefaultETL(getResourceUri("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
//        runDefaultETL(getResourceUri("1k.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"), variantStorageManager,
                studyConfiguration, options
                        .append(VariantStorageEngine.Options.LOAD_SPLIT_DATA.key(), true));
        int fileIdChr22 = studyConfiguration.getFileIds().get("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        assertTrue(studyConfiguration.getIndexedFiles().contains(fileIdChr22));
        checkLoadedVariants(getVariantStorageEngine().getDBAdaptor(), studyConfiguration, true, false, false, -1);

        assertEquals(studyConfiguration.getSamplesInFiles().get(fileIdChr1), studyConfiguration.getSamplesInFiles().get(fileIdChr22));

        //Check generated stats files
        assertEquals(2504, studyConfiguration.getCohorts().get(defaultCohortId).size());
        File[] statsFile1 = getTmpRootDir().toFile().listFiles((dir, name1) -> name1.startsWith(VariantStoragePipeline.buildFilename(studyConfiguration.getStudyName(), fileIdChr1))
                && name1.contains("variants"));
        File[] statsFile2 = getTmpRootDir().toFile().listFiles((dir, name1) -> name1.startsWith(VariantStoragePipeline.buildFilename(studyConfiguration.getStudyName(), fileIdChr22))
                && name1.contains("variants"));
        assertEquals(1, statsFile1.length);
        assertEquals(1, statsFile2.length);

        JsonFactory jsonFactory = new JsonFactory();
        ObjectMapper jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        try (JsonParser parser = jsonFactory.createParser(new GZIPInputStream(new FileInputStream(statsFile1[0])))) {
            while (parser.nextToken() != null) {
                VariantStatsWrapper variantStatsWrapper = parser.readValueAs(VariantStatsWrapper.class);
                assertEquals("1", variantStatsWrapper.getChromosome());
            }
        }
        try (JsonParser parser = jsonFactory.createParser(new GZIPInputStream(new FileInputStream(statsFile2[0])))) {
            while (parser.nextToken() != null) {
                VariantStatsWrapper variantStatsWrapper = parser.readValueAs(VariantStatsWrapper.class);
                assertEquals("22", variantStatsWrapper.getChromosome());
            }
        }


    }

    @Test
    public void multiRegionIndexFail() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = new StudyConfiguration(1, "multiRegion");

        ObjectMap options = new ObjectMap()
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.CONTROL_SET)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);

        runDefaultETL(getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
                studyConfiguration, options);

        studyConfiguration.getFileIds().put("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", 6);
        StorageEngineException exception = StorageEngineException.alreadyLoadedSamples(studyConfiguration, 6);
        thrown.expect(exception.getClass());
        thrown.expectMessage(exception.getMessage());
        runDefaultETL(getResourceUri("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
                studyConfiguration, options.append(VariantStorageEngine.Options.LOAD_SPLIT_DATA.key(), false));
    }

    /**
     * Single Thread indexation. "Old Style" indexation
     * With samples and "src"
     * Gzip compression
     **/
    @Test
    public void singleThreadIndex() throws Exception {
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap();
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        params.put(VariantStorageEngine.Options.STUDY.key(), studyConfiguration.getStudyName());
        params.put(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "json");
        params.put(VariantStorageEngine.Options.COMPRESS_METHOD.key(), "gZiP");
        params.put(VariantStorageEngine.Options.TRANSFORM_THREADS.key(), 1);
        params.put(VariantStorageEngine.Options.LOAD_THREADS.key(), 1);
//        params.put(VariantStorageEngine.Options.INCLUDE_GENOTYPES.key(), true);
//        params.put(VariantStorageEngine.Options.INCLUDE_SRC.key(), true);
        StoragePipelineResult etlResult = runETL(variantStorageEngine, params, true, true, true);
        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();

        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.json.gz'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.json.gz"));

        Integer fileId = studyConfiguration.getFileIds().get(UriUtils.fileName(inputUri));
        assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));
        VariantFileMetadata fileMetadata = checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, true, false, getExpectedNumLoadedVariants(fileMetadata));

    }

    /**
     * Fast indexation.
     * Without "src" and samples information.
     * MultiThreads
     * CompressMethod snappy
     **/
    @Test
    public void fastIndex() throws Exception {
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap();
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        params.put(VariantStorageEngine.Options.STUDY.key(), studyConfiguration.getStudyName());
        params.put(VariantStorageEngine.Options.COMPRESS_METHOD.key(), "snappy");
        params.put(VariantStorageEngine.Options.TRANSFORM_THREADS.key(), 8);
        params.put(VariantStorageEngine.Options.LOAD_THREADS.key(), 8);
//        params.put(VariantStorageEngine.Options.INCLUDE_GENOTYPES.key(), false);
//        params.put(VariantStorageEngine.Options.INCLUDE_SRC.key(), false);
        StoragePipelineResult etlResult = runETL(variantStorageEngine, params, true, true, true);

        System.out.println("etlResult = " + etlResult);
        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();

        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.avro.snappy'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.avro.snappy"));

        Integer fileId = studyConfiguration.getFileIds().get(UriUtils.fileName(inputUri));
        assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));
        VariantFileMetadata fileMetadata = checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, false, false, false, getExpectedNumLoadedVariants
                (fileMetadata));

    }

    @Test
    public void indexWithOtherFields() throws Exception {
        indexWithOtherFields("GL,DS");
    }

    @Test
    public void indexWithOtherFieldsALL() throws Exception {
        indexWithOtherFields(VariantQueryUtils.ALL);
    }

    @Test
    public void indexWithOtherFieldsAllByDefaultDefault() throws Exception {
        indexWithOtherFields(null);
    }

    public void indexWithOtherFields(String extraFields) throws Exception {
        //GT:DS:GL

        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields)
                        .append(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
        );

        VariantFileMetadata fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(etlResult.getTransformResult());
        checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration, fileMetadata.getStats().getNumVariants());
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        checkLoadedVariants(dbAdaptor, studyConfiguration, true, false, false, getExpectedNumLoadedVariants(fileMetadata));

        String fileId = studyConfiguration.getFileIds().get(UriUtils.fileName(smallInputUri)).toString();
        VariantReader reader = VariantReaderUtils.getVariantReader(Paths.get(etlResult.getTransformResult().getPath()),
                new VariantFileMetadata(fileId, "").toVariantStudyMetadata(String.valueOf(studyConfiguration.getStudyId())));

        reader.open();
        reader.pre();
        for (Variant variant : reader.read(999)) {
            StudyEntry studyEntry = variant.getStudies().get(0);
            studyEntry.setStudyId(STUDY_NAME);
            studyEntry.getFiles().get(0).setFileId(fileId);
            variant.setStudies(Collections.singletonList(studyEntry));

            Variant loadedVariant = dbAdaptor.get(new Query(VariantQueryParam.ID.key(), variant.toString())
                    .append(VariantQueryParam.INCLUDE_FORMAT.key(), "GT,GL,DS"), new QueryOptions()).first();

            loadedVariant.setAnnotation(null);                                          //Remove annotation
            StudyEntry loadedStudy = loadedVariant.getStudy(STUDY_NAME);
            loadedStudy.setStats(Collections.emptyMap());        //Remove calculated stats
            loadedStudy.getSamplesData().forEach(values -> {
                values.set(0, values.get(0).replace("0/0", "0|0"));
                while (values.get(2).length() < 5) values.set(2, values.get(2) + "0");   //Set lost zeros
            });
            for (FileEntry fileEntry : loadedStudy.getFiles()) {
                if(StringUtils.isEmpty(fileEntry.getCall())) {
                    fileEntry.setCall(null);
                }
            }
            variant.resetLength();
            assertEquals("\n" + variant.toJson() + "\n" + loadedVariant.toJson(), variant, loadedVariant);

        }
        dbAdaptor.close();
        reader.post();
        reader.close();

    }

    @Test
    public void indexWithoutOtherFields() throws Exception {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        runDefaultETL(smallInputUri, getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), VariantQueryUtils.NONE)
                        .append(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
        );
        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertEquals("GT", variant.getStudy(STUDY_NAME).getFormatAsString());
        }
    }

    /* ---------------------------------------------------- */
    /* Check methods for loaded and transformed Variants    */
    /* ---------------------------------------------------- */


    private VariantFileMetadata checkTransformedVariants(URI variantsJson, StudyConfiguration studyConfiguration) throws StorageEngineException {
        return checkTransformedVariants(variantsJson, studyConfiguration, -1);
    }

    private VariantFileMetadata checkTransformedVariants(URI variantsJson, StudyConfiguration studyConfiguration, int expectedNumVariants)
            throws StorageEngineException {
        long start = System.currentTimeMillis();
        VariantFileMetadata source = new VariantFileMetadata("6", VCF_TEST_FILE_NAME);
        VariantReader variantReader = VariantReaderUtils.getVariantReader(Paths.get(variantsJson.getPath()), source.toVariantStudyMetadata(String.valueOf(studyConfiguration.getStudyId())));

        variantReader.open();
        variantReader.pre();

        List<Variant> read;
        int numVariants = 0;
        while ((read = variantReader.read(100)) != null && !read.isEmpty()) {
            numVariants += read.size();
        }

        variantReader.post();
        variantReader.close();

        if (expectedNumVariants < 0) {
            expectedNumVariants = source.getStats().getNumVariants();
        } else {
            assertEquals(expectedNumVariants, source.getStats().getNumVariants()); //9792
        }
        assertEquals(expectedNumVariants, numVariants); //9792
        logger.info("checkTransformedVariants time : " + (System.currentTimeMillis() - start) / 1000.0 + "s");

        return source;
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, boolean includeSamples, boolean
            includeSrc) {
        checkLoadedVariants(dbAdaptor, studyConfiguration, includeSamples, includeSrc, NUM_VARIANTS/*9792*/);
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration,
                                     boolean includeSamples, boolean includeSrc, int expectedNumVariants) {
        checkLoadedVariants(dbAdaptor, studyConfiguration, includeSamples, includeSrc, false, expectedNumVariants);
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration,
                                     boolean includeSamples, boolean includeSrc, boolean includeAnnotation, int expectedNumVariants) {
        long start = System.currentTimeMillis();
        int numVariants = 0;
        String expectedStudyId = studyConfiguration.getStudyName();
        QueryResult<Long> count = dbAdaptor.count(new Query());
        assertEquals(1, count.getNumResults());
        if (expectedNumVariants >= 0) {
            assertEquals(expectedNumVariants, count.first().intValue());
        }
//        for (Integer fileId : studyConfiguration.getIndexedFiles()) {
//            assertTrue(studyConfiguration.getHeaders().containsKey(fileId));
//        }
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                if (!entry.getValue().getStudyId().equals(expectedStudyId)) {
                    continue;
                } else {
                    numVariants++;
                }
                assertEquals(expectedStudyId, entry.getValue().getStudyId());
                if (includeSamples) {
                    assertNotNull(entry.getValue().getSamplesData());
                    assertEquals(studyConfiguration.getSampleIds().size(), entry.getValue().getSamplesData().size());

                    assertEquals(studyConfiguration.getSampleIds().size(), entry.getValue().getSamplesData().size());
                    assertEquals(studyConfiguration.getSampleIds().keySet(), entry.getValue().getSamplesDataAsMap().keySet());
                }
                for (FileEntry fileEntry : entry.getValue().getFiles()) {
                    if (includeSrc) {
                        assertNotNull(fileEntry.getAttributes().get(VariantVcfFactory.SRC));
                    } else {
                        assertNull(fileEntry.getAttributes().getOrDefault(VariantVcfFactory.SRC, null));
                    }
                }
                for (Integer cohortId : studyConfiguration.getCalculatedStats()) {
                    try {
                        String cohortName = StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).get(cohortId);
                        assertTrue(entry.getValue().getStats().containsKey(cohortName));
                        assertEquals(variant + " has incorrect stats for cohort \"" + cohortName + "\":" + cohortId,
                                studyConfiguration.getCohorts().get(cohortId).size(),
                                entry.getValue().getStats().get(cohortName).getGenotypesCount().values().stream().reduce((a, b) -> a + b)
                                        .orElse(0).intValue());
                    } catch (AssertionError error) {
                        System.out.println(variant + " = " + variant.toJson());
                        throw error;
                    }
                }
                if (includeAnnotation) {
                    assertNotNull(variant.toString(), variant.getAnnotation());
                }
            }
        }
        if (expectedNumVariants >= 0) {
            assertEquals(expectedNumVariants, numVariants);
        }
        logger.info("checkLoadedVariants time : " + (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    @Test
    @Ignore
    public void insertVariantIntoSolr() throws Exception {
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap();
        StudyConfiguration studyConfiguration = newStudyConfiguration();

        params.put(VariantStorageEngine.Options.STUDY.key(), studyConfiguration.getStudyName());
        params.put(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "json");
        params.put(VariantStorageEngine.Options.COMPRESS_METHOD.key(), "gZiP");
        params.put(VariantStorageEngine.Options.TRANSFORM_THREADS.key(), 1);
        params.put(VariantStorageEngine.Options.LOAD_THREADS.key(), 1);
        params.put(VariantStorageEngine.Options.ANNOTATE.key(), true);
        runETL(variantStorageEngine, params, true, true, true);

        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();


        VariantSearchManager variantSearchManager = new VariantSearchManager(null, variantStorageEngine.getConfiguration());
        // FIXME Collection is not in the configuration any more
//        variantSearchManager.load(variantStorageEngine.getConfiguration().getSearch().getCollection(), dbAdaptor.iterator());
    }


    @Test
    public void removeFileTest() throws Exception {
        removeFileTest(new QueryOptions());
    }

    public void removeFileTest(QueryOptions params) throws Exception {
        StudyConfiguration studyConfiguration1 = new StudyConfiguration(1, "Study1");
        StudyConfiguration studyConfiguration2 = new StudyConfiguration(2, "Study2");

        ObjectMap options = new ObjectMap(params)
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.CONTROL_SET)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);
        //Study1
        runDefaultETL(getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration1, options);
        runDefaultETL(getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration1, options);

        //Study2
        runDefaultETL(getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration2, options);
        runDefaultETL(getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration2, options);
        runDefaultETL(getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration2, options);

        variantStorageEngine.removeFile(studyConfiguration1.getStudyName(), 2);

        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertFalse(variant.getStudies().isEmpty());
            StudyEntry study = variant.getStudy("1");
            if (study != null) {
                List<FileEntry> files = study.getFiles();
                assertEquals(1, files.size());
                assertEquals("1", files.get(0).getFileId());
            }
        }

        variantStorageEngine.getDBAdaptor().getStudyConfigurationManager().variantFileMetadataIterator(new Query(), new QueryOptions())
                .forEachRemaining(vs -> assertNotEquals("2", vs.getId()));
    }

    @Test
    public void loadSVFilesTest() throws Exception {
        URI input = getResourceUri("variant-test-sv.vcf");
        StudyConfiguration studyConfiguration = new StudyConfiguration(1, "s1");
        runDefaultETL(input, variantStorageEngine, studyConfiguration, new QueryOptions()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), true));
        URI input2 = getResourceUri("variant-test-sv_2.vcf");
        runDefaultETL(input2, variantStorageEngine, studyConfiguration, new QueryOptions()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), true));

        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            if (variant.getAlternate().equals("<DEL:ME:ALU>") || variant.getType().equals(VariantType.BREAKEND)) {
                System.err.println("WARN: Variant " + variant + (variant.getAnnotation() == null ? " without annotation" : " with annotation"));
            } else {
                assertNotNull(variant.toString(), variant.getAnnotation());
            }
        }

        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, true, false, false, 24 + 7);

        variantStorageEngine.exportData(null, VariantWriterFactory.VariantOutputFormat.VCF, new Query(), new QueryOptions(QueryOptions.SORT, true));
    }

}
