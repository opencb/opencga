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
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;
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
public abstract class VariantStorageManagerTest extends VariantStorageBaseTest {

    private static Logger logger = LoggerFactory.getLogger(VariantStorageManagerTest.class);
    @Test
    public void basicIndex() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "json"));
        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.json.gz'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.json.gz"));
        VariantSource source = VariantReaderUtils.readVariantSource(Paths.get(etlResult.getTransformResult().getPath()), null);

        assertTrue(studyConfiguration.getIndexedFiles().contains(6));
        checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, true, false, getExpectedNumLoadedVariants(source));
    }

    @Test
    public void avroBasicIndex() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, variantStorageEngine, studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro"));
        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.avro.gz'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.avro.gz"));

        assertTrue(studyConfiguration.getIndexedFiles().contains(6));
        VariantSource variantSource = checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, true, false, getExpectedNumLoadedVariants
                (variantSource));
    }

    @Test
    public void multiIndex() throws Exception {
        clearDB(DB_NAME);
        int expectedNumVariants = NUM_VARIANTS - 37; //37 variants have been removed from this dataset because had the genotype 0|0 for
        // each sample
        StudyConfiguration studyConfigurationMultiFile = new StudyConfiguration(1, "multi");

        StoragePipelineResult etlResult;
        ObjectMap options = new ObjectMap()
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), VariantStudy.StudyType.CONTROL)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);
        runDefaultETL(getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfigurationMultiFile, options.append(VariantStorageEngine.Options.FILE_ID.key(), 5));
        Integer defaultCohortId = studyConfigurationMultiFile.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
        assertTrue(studyConfigurationMultiFile.getCohorts().containsKey(defaultCohortId));
        assertEquals(500, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertEquals(Collections.emptySet(), studyConfigurationMultiFile.getCalculatedStats());
        assertEquals(Collections.emptySet(), studyConfigurationMultiFile.getInvalidStats());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(5));

        options.append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);
        runDefaultETL(getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfigurationMultiFile, options.append(VariantStorageEngine.Options.FILE_ID.key(), 6));
        assertEquals(1000, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertEquals(Collections.singleton(defaultCohortId), studyConfigurationMultiFile.getCalculatedStats());
        assertEquals(Collections.emptySet(), studyConfigurationMultiFile.getInvalidStats());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(6));

        options.append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);
        runDefaultETL(getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfigurationMultiFile, options.append(VariantStorageEngine.Options.FILE_ID.key(), 7));
        assertEquals(1500, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertEquals(Collections.emptySet(), studyConfigurationMultiFile.getCalculatedStats());
        assertEquals(Collections.singleton(defaultCohortId), studyConfigurationMultiFile.getInvalidStats());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(7));

        options.append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);
        runDefaultETL(getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfigurationMultiFile, options.append(VariantStorageEngine.Options.FILE_ID.key(), 8));
        assertEquals(2000, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(8));
        runDefaultETL(getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfigurationMultiFile, options.append(VariantStorageEngine.Options.FILE_ID.key(), 9));
        assertEquals(2504, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(5));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(6));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(7));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(8));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(9));

        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        checkLoadedVariants(dbAdaptor, studyConfigurationMultiFile, true, false, expectedNumVariants);


        //Load, in a new study, the same dataset in one single file
        StudyConfiguration studyConfigurationSingleFile = new StudyConfiguration(2, "single");
        etlResult = runDefaultETL(getResourceUri("filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfigurationSingleFile, options.append(VariantStorageEngine.Options.FILE_ID.key(), 10));
        assertTrue(studyConfigurationSingleFile.getIndexedFiles().contains(10));

        checkTransformedVariants(etlResult.getTransformResult(), studyConfigurationSingleFile);


        //Check that both studies contains the same information
        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantQueryParam.STUDIES.key(),
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

        options.putIfAbsent(VariantStorageEngine.Options.STUDY_TYPE.key(), VariantStudy.StudyType.COLLECTION);
        options.putIfAbsent(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);
        options.putIfAbsent(VariantStorageEngine.Options.ANNOTATE.key(), false);

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();
        StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
        int i = 1;
        for (int fileId = 77; fileId <= 93; fileId++) {
            ObjectMap fileOptions = new ObjectMap();
            fileOptions.append(VariantStorageEngine.Options.SAMPLE_IDS.key(), "NA128" + fileId + ':' + (i - 1))
                    .append(VariantStorageEngine.Options.FILE_ID.key(), i)
                    .putAll(options);
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
                .append(VariantStorageEngine.Options.STUDY_NAME.key(), studyConfigurationBatchFile.getStudyName())
                .append(VariantStorageEngine.Options.STUDY_ID.key(), studyConfigurationBatchFile.getStudyId())
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
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), VariantStudy.StudyType.CONTROL)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();

        URI chr1 = getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI chr22 = getResourceUri("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

//        runDefaultETL(chr1, this.variantStorageManager,
//                studyConfiguration, options.append(VariantStorageEngine.Options.FILE_ID.key(), 5));
//        Integer defaultCohortId = studyConfiguration.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
//        assertTrue(studyConfiguration.getCohorts().containsKey(defaultCohortId));
//        assertEquals(2504, studyConfiguration.getCohorts().get(defaultCohortId).size());
//        assertTrue(studyConfiguration.getIndexedFiles().contains(5));
//        checkLoadedVariants(variantStorageManager.getDBAdaptor(), studyConfiguration, true, false, -1);
//
//        runDefaultETL(chr22, this.variantStorageManager,
////        runDefaultETL(getResourceUri("1k.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"), variantStorageManager,
//                studyConfiguration, options.append(VariantStorageEngine.Options.FILE_ID.key(), 6));

        variantStorageManager.getOptions()
                .append(VariantStorageEngine.Options.STUDY_NAME.key(), STUDY_NAME)
                .append(VariantStorageEngine.Options.STUDY_ID.key(), STUDY_ID);

        List<StoragePipelineResult> results = variantStorageManager.index(Arrays.asList(chr1, chr22), outputUri, true, true, true);

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

        StoragePipelineResult etlResult;
        ObjectMap options = new ObjectMap()
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), VariantStudy.StudyType.CONTROL)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);

        runDefaultETL(getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
                studyConfiguration, options.append(VariantStorageEngine.Options.FILE_ID.key(), 5));
        Integer defaultCohortId = studyConfiguration.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
        assertTrue(studyConfiguration.getCohorts().containsKey(defaultCohortId));
        assertEquals(2504, studyConfiguration.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfiguration.getIndexedFiles().contains(5));
        checkLoadedVariants(getVariantStorageEngine().getDBAdaptor(), studyConfiguration, true, false, -1);

        runDefaultETL(getResourceUri("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
//        runDefaultETL(getResourceUri("1k.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"), variantStorageManager,
                studyConfiguration, options.append(VariantStorageEngine.Options.FILE_ID.key(), 6));

        assertTrue(studyConfiguration.getIndexedFiles().contains(6));
        checkLoadedVariants(getVariantStorageEngine().getDBAdaptor(), studyConfiguration, true, false, -1);

        assertEquals(studyConfiguration.getSamplesInFiles().get(5), studyConfiguration.getSamplesInFiles().get(6));

        //Check generated stats files
        assertEquals(2504, studyConfiguration.getCohorts().get(defaultCohortId).size());
        File[] statsFile1 = getTmpRootDir().toFile().listFiles((dir, name1) -> name1.startsWith(VariantStoragePipeline.buildFilename(studyConfiguration.getStudyName(), 5))
                && name1.contains("variants"));
        File[] statsFile2 = getTmpRootDir().toFile().listFiles((dir, name1) -> name1.startsWith(VariantStoragePipeline.buildFilename(studyConfiguration.getStudyName(), 6))
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
        params.put(VariantStorageEngine.Options.STUDY_ID.key(), studyConfiguration.getStudyId());
        params.put(VariantStorageEngine.Options.STUDY_NAME.key(), studyConfiguration.getStudyName());
        params.put(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "json");
        params.put(VariantStorageEngine.Options.FILE_ID.key(), 6);
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

        assertTrue(studyConfiguration.getIndexedFiles().contains(6));
        VariantSource source = checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, true, false, getExpectedNumLoadedVariants(source));

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
        params.put(VariantStorageEngine.Options.STUDY_ID.key(), studyConfiguration.getStudyId());
        params.put(VariantStorageEngine.Options.STUDY_NAME.key(), studyConfiguration.getStudyName());
        params.put(VariantStorageEngine.Options.FILE_ID.key(), 6);
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

        assertTrue(studyConfiguration.getIndexedFiles().contains(6));
        VariantSource variantSource = checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyConfiguration, false, false, getExpectedNumLoadedVariants
                (variantSource));

    }

    @Test
    public void indexWithOtherFields() throws Exception {
        //GT:DS:GL

        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GL", "DS"))
                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
        );

        VariantSource source = VariantReaderUtils.readVariantSource(Paths.get(etlResult.getTransformResult().getPath()), null);
        checkTransformedVariants(etlResult.getTransformResult(), studyConfiguration, source.getStats().getNumRecords());
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        checkLoadedVariants(dbAdaptor, studyConfiguration, true, false, getExpectedNumLoadedVariants(source));

        VariantReader reader = VariantReaderUtils.getVariantReader(Paths.get(etlResult.getTransformResult().getPath()),
                new VariantSource("", "2", STUDY_NAME, STUDY_NAME));

        reader.open();
        reader.pre();
        for (Variant variant : reader.read(999)) {
            if (variant.getAlternate().startsWith("<") || variant.getStart().equals(70146475) || variant.getStart().equals(107976940)) {
                continue;
            }
            StudyEntry studyEntry = variant.getStudies().get(0);
            studyEntry.setStudyId(STUDY_NAME);
            studyEntry.getFiles().get(0).setFileId("2");
            variant.setStudies(Collections.singletonList(studyEntry));

            Variant loadedVariant = dbAdaptor.get(new Query(VariantQueryParam.ID.key(), variant.toString()), new QueryOptions()).first();

            loadedVariant.setAnnotation(null);                                          //Remove annotation
            StudyEntry loadedStudy = loadedVariant.getStudy(STUDY_NAME);
            loadedStudy.setFormat(Arrays.asList(loadedStudy.getFormat().get(0), loadedStudy.getFormat().get(2), loadedStudy.getFormat().get(1)));
            loadedStudy.setStats(Collections.emptyMap());        //Remove calculated stats
            loadedStudy.getSamplesData().forEach(values -> {
                values.set(0, values.get(0).replace("0/0", "0|0"));
                String v1 = values.get(1);
                values.set(1, values.get(2));
                values.set(2, v1);
                while (values.get(2).length() < 5) values.set(2, values.get(2) + "0");   //Set lost zeros
            });
            variant.resetLength();
            assertEquals("\n" + variant.toJson() + "\n" + loadedVariant.toJson(), variant, loadedVariant);

        }
        dbAdaptor.close();
        reader.post();
        reader.close();

    }

    @Test
    public void indexWithOtherFieldsNoGT() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult etlResult = runDefaultETL(getResourceUri("variant-test-somatic.vcf"), getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU"))
                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );

        VariantDBIterator iterator = getVariantStorageEngine().getDBAdaptor().iterator(new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            assertEquals("./.", variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "DP"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GL"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "AU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "CU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "TU"));
        }

    }

    @Test
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        List<String> extraFields = Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU");
        StoragePipelineResult etlResult = runDefaultETL(getResourceUri("variant-test-somatic.vcf"), getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields)
                        .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), false)
                        .append(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), true)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );
        etlResult = runDefaultETL(getResourceUri("variant-test-somatic_2.vcf"), getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields)
                        .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), true)
                        .append(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                        .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );
        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        assertEquals(true, studyConfiguration.getAttributes().getBoolean(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), false));
        assertEquals(extraFields, studyConfiguration.getAttributes().getAsStringList(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key()));

        for (Variant variant : dbAdaptor) {
            System.out.println(variant.toJson());
            assertNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "DP"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GL"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "AU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "CU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "TU"));
        }

        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantQueryParam.RETURNED_SAMPLES.key(), "SAMPLE_1"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() > 0);
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() <= 2);

        });

        iterator = dbAdaptor.iterator(new Query(VariantQueryParam.RETURNED_SAMPLES.key(), "SAMPLE_2"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_2"), variant.getStudy(STUDY_NAME).getSamplesName());
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() > 0);
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() <= 2);

        });

        iterator = dbAdaptor.iterator(new Query(VariantQueryParam.RETURNED_SAMPLES.key(), "SAMPLE_2")
                        .append(VariantQueryParam.FILES.key(), 3)
                        .append(VariantQueryParam.RETURNED_FILES.key(), 3), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            System.out.println("variant.toJson() = " + variant.toJson());
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_2"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("3", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
    }

    /* ---------------------------------------------------- */
    /* Check methods for loaded and transformed Variants    */
    /* ---------------------------------------------------- */


    private VariantSource checkTransformedVariants(URI variantsJson, StudyConfiguration studyConfiguration) throws StorageEngineException {
        return checkTransformedVariants(variantsJson, studyConfiguration, -1);
    }

    private VariantSource checkTransformedVariants(URI variantsJson, StudyConfiguration studyConfiguration, int expectedNumVariants)
            throws StorageEngineException {
        long start = System.currentTimeMillis();
        VariantSource source = new VariantSource(VCF_TEST_FILE_NAME, "6", "", "");
        VariantReader variantReader = VariantReaderUtils.getVariantReader(Paths.get(variantsJson.getPath()), source);

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
            expectedNumVariants = source.getStats().getNumRecords();
        } else {
            assertEquals(expectedNumVariants, source.getStats().getNumRecords()); //9792
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
                    String cohortName = StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).get(cohortId);
                    assertTrue(entry.getValue().getStats().containsKey(cohortName));
                    assertEquals(variant + " has incorrect stats for cohort \"" + cohortName + "\":" + cohortId,
                            studyConfiguration.getCohorts().get(cohortId).size(),
                            entry.getValue().getStats().get(cohortName).getGenotypesCount().values().stream().reduce((a, b) -> a + b)
                                    .orElse(0).intValue());
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

        params.put(VariantStorageEngine.Options.STUDY_ID.key(), studyConfiguration.getStudyId());
        params.put(VariantStorageEngine.Options.STUDY_NAME.key(), studyConfiguration.getStudyName());
        params.put(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "json");
        params.put(VariantStorageEngine.Options.FILE_ID.key(), 6);
        params.put(VariantStorageEngine.Options.COMPRESS_METHOD.key(), "gZiP");
        params.put(VariantStorageEngine.Options.TRANSFORM_THREADS.key(), 1);
        params.put(VariantStorageEngine.Options.LOAD_THREADS.key(), 1);
        params.put(VariantStorageEngine.Options.ANNOTATE.key(), true);
        runETL(variantStorageEngine, params, true, true, true);

        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();


        VariantSearchManager variantSearchManager = new VariantSearchManager(null, null, variantStorageEngine.getConfiguration());
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
                .append(VariantStorageEngine.Options.STUDY_TYPE.key(), VariantStudy.StudyType.CONTROL)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false);
        //Study1
        runDefaultETL(getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration1, options.append(VariantStorageEngine.Options.FILE_ID.key(), 1));
        runDefaultETL(getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration1, options.append(VariantStorageEngine.Options.FILE_ID.key(), 2));

        //Study2
        runDefaultETL(getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration2, options.append(VariantStorageEngine.Options.FILE_ID.key(), 3));
        runDefaultETL(getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration2, options.append(VariantStorageEngine.Options.FILE_ID.key(), 4));
        runDefaultETL(getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyConfiguration2, options.append(VariantStorageEngine.Options.FILE_ID.key(), 5));

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

        variantStorageEngine.getDBAdaptor().getVariantSourceDBAdaptor().iterator(new Query(), new QueryOptions()).forEachRemaining(vs -> {
            assertNotEquals("2", vs.getFileId());
        });



    }

}
