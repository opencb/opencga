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
import com.google.common.collect.Iterators;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.TestIOConnector;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.*;
import static org.opencb.opencga.core.common.UriUtils.dirName;
import static org.opencb.opencga.core.common.UriUtils.fileName;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageEngineTest extends VariantStorageBaseTest {

    private static Logger logger = LoggerFactory.getLogger(VariantStorageEngineTest.class);

    @Test
    public void basicIndex() throws Exception {

        clearDB(DB_NAME);
        StudyMetadata studyMetadata = newStudyMetadata();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata,
                new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "json"));
        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.json.gz'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.json.gz"));
        VariantFileMetadata fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(etlResult.getTransformResult());
        assertEquals(1, metadataManager.getIndexedFiles(studyMetadata.getId()).size());
        checkTransformedVariants(etlResult.getTransformResult(), studyMetadata);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyMetadata, true, false, true, getExpectedNumLoadedVariants(fileMetadata));
    }

    @Test
    public void avroBasicIndex() throws Exception {
        clearDB(DB_NAME);
        StudyMetadata studyMetadata = newStudyMetadata();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata,
                new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro"));
        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.avro.gz'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.avro.gz"));

        assertEquals(1, metadataManager.getIndexedFiles(studyMetadata.getId()).size());
        VariantFileMetadata fileMetadata = checkTransformedVariants(etlResult.getTransformResult(), studyMetadata);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyMetadata, true, false, true, getExpectedNumLoadedVariants
                (fileMetadata));
    }

    @Test
    public void loadFromSTDIN() throws Exception {
        clearDB(DB_NAME);
        StudyMetadata studyMetadata = newStudyMetadata();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata,
                new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro"), true, false);

        VariantFileMetadata fileMetadata = checkTransformedVariants(etlResult.getTransformResult(), studyMetadata);

        Path tempFile = Paths.get(outputUri).resolve("temp_file");
        Files.move(Paths.get(etlResult.getTransformResult()), tempFile);
        assertFalse(Files.exists(Paths.get(etlResult.getTransformResult())));

        InputStream in = System.in;
        try (InputStream is = new FileInputStream(tempFile.toFile())) {
            System.setIn(is);

            variantStorageEngine.getConfiguration()
                    .getVariantEngine(variantStorageEngine.getStorageEngineId()).getOptions()
                    .put(VariantStorageOptions.STDIN.key(), true);
            variantStorageEngine.index(Collections.singletonList(etlResult.getTransformResult()), outputUri, false, false, true);

        } finally {
            System.setIn(in);
        }

        studyMetadata = metadataManager.getStudyMetadata(STUDY_NAME);

        assertEquals(1, metadataManager.getIndexedFiles(studyMetadata.getId()).size());
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyMetadata, true, false, true,
                getExpectedNumLoadedVariants(fileMetadata));
    }

    @Test
    public void externalIOManager() throws Exception {
        clearDB(DB_NAME);

        variantStorageEngine.getIOManagerProvider().add(new TestIOConnector());
        variantReaderUtils.getIOConnectorProvider().add(new TestIOConnector());
        StudyMetadata studyMetadata = newStudyMetadata();


//        URI input = VariantStorageBaseTest.smallInputUri;
        URI input = URI.create("test://localhost/").resolve(dirName(smallInputUri)).resolve(fileName(smallInputUri));

        URI outputUri = URI.create("test://localhost/").resolve(dirName(VariantStorageBaseTest.outputUri));
        StoragePipelineResult etlResult = runETL(variantStorageEngine,
                input,
                outputUri,
                new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), true)
                        .append(VariantStorageOptions.ANNOTATE.key(), true)
//                        .append("annotation.file.avro", "true")
                        .append(VariantStorageOptions.SPECIES.key(), "hsapiens")
                        .append(VariantStorageOptions.ASSEMBLY.key(), "grch37"),
                true, true, true);

        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.avro.gz'",
                fileName(etlResult.getTransformResult()).endsWith("variants.avro.gz"));
        VariantFileMetadata fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(etlResult.getTransformResult());
        assertEquals(1, metadataManager.getIndexedFiles(studyMetadata.getId()).size());
        checkTransformedVariants(etlResult.getTransformResult(), studyMetadata);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyMetadata, true, false, true, getExpectedNumLoadedVariants(fileMetadata));


        URI exportOutput = outputUri.resolve("export.vcf.gz");
        variantStorageEngine.exportData(exportOutput, VariantWriterFactory.VariantOutputFormat.VCF_GZ, null,
                new Query(), new QueryOptions(QueryOptions.SORT, true));

        assertTrue(ioConnectorProvider.exists(exportOutput));
    }

    @Test
    public void multiIndex() throws Exception {
        clearDB(DB_NAME);
        int expectedNumVariants = NUM_VARIANTS - 37; //37 variants have been removed from this dataset because had the genotype 0|0 for
        // each sample
        StudyMetadata studyMetadataMultiFile = new StudyMetadata(1, "multi");

        StoragePipelineResult etlResult;
        ObjectMap options = new ObjectMap()
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.ANNOTATE.key(), false);
        URI file1Uri = getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file1Uri, variantStorageEngine, studyMetadataMultiFile, options);
        CohortMetadata defaultCohort = metadataManager.getCohortMetadata(studyMetadataMultiFile.getId(), StudyEntry.DEFAULT_COHORT);
        
        assertNotNull(defaultCohort);
        assertEquals(500, defaultCohort.getSamples().size());
        assertFalse(defaultCohort.isStatsReady());
        assertFalse(defaultCohort.isInvalid());
        Integer fileId1 = metadataManager.getFileId(studyMetadataMultiFile.getId(), file1Uri);
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId1));

        options.append(VariantStorageOptions.STATS_CALCULATE.key(), true);
        URI file2Uri = getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file2Uri, variantStorageEngine, studyMetadataMultiFile, options);
        defaultCohort = metadataManager.getCohortMetadata(studyMetadataMultiFile.getId(), StudyEntry.DEFAULT_COHORT);
        assertEquals(1000, defaultCohort.getSamples().size());
        assertTrue(defaultCohort.isStatsReady());
        assertFalse(defaultCohort.isInvalid());
        Integer fileId2 = metadataManager.getFileId(studyMetadataMultiFile.getId(), file2Uri);
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId2));

        options.append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        URI file3Uri = getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file3Uri, variantStorageEngine, studyMetadataMultiFile, options);
        defaultCohort = metadataManager.getCohortMetadata(studyMetadataMultiFile.getId(), StudyEntry.DEFAULT_COHORT);
        assertEquals(1500, defaultCohort.getSamples().size());
        assertFalse(defaultCohort.isStatsReady());
        assertTrue(defaultCohort.isInvalid());
        int fileId3 = metadataManager.getFileId(studyMetadataMultiFile.getId(), file3Uri);
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId3));

        options.append(VariantStorageOptions.STATS_CALCULATE.key(), true);
        URI file4Uri = getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file4Uri, variantStorageEngine, studyMetadataMultiFile, options);
        defaultCohort = metadataManager.getCohortMetadata(studyMetadataMultiFile.getId(), StudyEntry.DEFAULT_COHORT);
        assertEquals(2000, defaultCohort.getSamples().size());
        int fileId4 = metadataManager.getFileId(studyMetadataMultiFile.getId(), file4Uri);
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId4));

        URI file5Uri = getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        runDefaultETL(file5Uri, variantStorageEngine, studyMetadataMultiFile, options);
        int fileId5 = metadataManager.getFileId(studyMetadataMultiFile.getId(), file5Uri);
        defaultCohort = metadataManager.getCohortMetadata(studyMetadataMultiFile.getId(), StudyEntry.DEFAULT_COHORT);
        assertEquals(2504, defaultCohort.getSamples().size());
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId1));
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId2));
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId3));
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId4));
        assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(fileId5));

        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        checkLoadedVariants(dbAdaptor, studyMetadataMultiFile, true, false, false, expectedNumVariants);


        //Load, in a new study, the same dataset in one single file
        StudyMetadata studyMetadataSingleFile = new StudyMetadata(2, "single");
        URI fileUri = getResourceUri("filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        etlResult = runDefaultETL(fileUri, variantStorageEngine, studyMetadataSingleFile, options);
        int fileId = metadataManager.getFileId(studyMetadataSingleFile.getId(), fileUri);
        assertTrue(metadataManager.getIndexedFiles(studyMetadataSingleFile.getId()).contains(fileId));

        checkTransformedVariants(etlResult.getTransformResult(), studyMetadataSingleFile);


        //Check that both studies contains the same information
        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantQueryParam.STUDY.key(),
                studyMetadataMultiFile.getId() + "," + studyMetadataSingleFile.getId()).append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "0/0"), new QueryOptions());
        int numVariants = 0;
        for (; iterator.hasNext(); ) {
            Variant variant = iterator.next();
            numVariants++;
//            Map<String, VariantSourceEntry> map = variant.getStudies().stream().collect(Collectors.toMap
// (VariantSourceEntry::getStudyId, Function.<VariantSourceEntry>identity()));
            Map<String, StudyEntry> map = variant.getStudiesMap();

            assertTrue(variant.toString(), map.containsKey(studyMetadataMultiFile.getName()));
            assertTrue(variant.toString(), map.containsKey(studyMetadataSingleFile.getName()));
            String expected = map.get(studyMetadataSingleFile.getName()).getSamplesData().toString();
            String actual = map.get(studyMetadataMultiFile.getName()).getSamplesData().toString();
            if (!assertWithConflicts(variant, () -> assertEquals(variant.toString(), expected, actual))) {
                List<List<String>> samplesDataSingle = map.get(studyMetadataSingleFile.getName()).getSamplesData();
                List<List<String>> samplesDataMulti = map.get(studyMetadataMultiFile.getName()).getSamplesData();
                for (int i = 0; i < samplesDataSingle.size(); i++) {
                    String sampleName = map.get(studyMetadataMultiFile.getName()).getOrderedSamplesName().get(i);
                    String message = variant.toString()
                            + " sample: " + sampleName
                            + " id " + metadataManager.getSampleId(studyMetadataMultiFile.getId(), sampleName);
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
        StudyMetadata studyMetadataMultiFile = new StudyMetadata(1, "multi");
        StudyMetadata studyMetadataBatchFile = new StudyMetadata(2, "batch");

        options.putIfAbsent(VariantStorageOptions.STATS_CALCULATE.key(), false);
        options.putIfAbsent(VariantStorageOptions.ANNOTATE.key(), false);

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();
        VariantStorageMetadataManager variantStorageMetadataManager = dbAdaptor.getMetadataManager();
        int i = 1;
        for (int fileId = 77; fileId <= 93; fileId++) {
            ObjectMap fileOptions = new ObjectMap();
            fileOptions.putAll(options);
            runDefaultETL(getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA128" + fileId + "_S1.genome.vcf.gz"),
                    variantStorageManager, studyMetadataMultiFile, fileOptions);
            assertTrue(metadataManager.getIndexedFiles(studyMetadataMultiFile.getId()).contains(i));
            i++;
        }


        List<URI> uris = new LinkedList<>();
        for (int fileId = 77; fileId <= 93; fileId++) {
            uris.add(getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA128" + fileId + "_S1.genome.vcf.gz"));
        }

        variantStorageManager = getVariantStorageEngine();
        variantStorageManager.getOptions()
                .append(VariantStorageOptions.STUDY.key(), studyMetadataBatchFile.getName())
                .putAll(options);

        List<StoragePipelineResult> results = variantStorageManager.index(uris, outputUri, true, true, true);

        for (StoragePipelineResult result : results) {
            System.out.println(result.toString());
            assertTrue(result.isTransformExecuted());
            assertNull(result.getTransformError());
            assertTrue(result.isLoadExecuted());
            assertNull(result.getLoadError());
        }

        checkLoadedVariants(dbAdaptor, studyMetadataBatchFile, true, false, -1);


        dbAdaptor.close();
        variantStorageMetadataManager.close();

//
//        //Load, in a new study, the same dataset in one single file
//        StudyMetadata studyMetadataSingleFile = new StudyMetadata(2, "single");
//        etlResult = runDefaultETL(getResourceUri("filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
//                variantStorageManager, studyMetadataSingleFile, options.append(VariantStorageEngine.Options.FILE_ID.key(), 10));
//        assertTrue(studyMetadataSingleFile.getIndexedFiles().contains(10));
//
//        checkTransformedVariants(etlResult.getTransformResult(), studyMetadataSingleFile);
//
//
//        //Check that both studies contains the same information
//        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(),
//                studyMetadataMultiFile.getStudyId() + "," + studyMetadataSingleFile.getStudyId()), new QueryOptions());
//        int numVariants = 0;
//        for (; iterator.hasNext(); ) {
//            Variant variant = iterator.next();
//            numVariants++;
////            Map<String, VariantSourceEntry> map = variant.getStudies().stream().collect(Collectors.toMap
//// (VariantSourceEntry::getStudyId, Function.<VariantSourceEntry>identity()));
//            Map<String, StudyEntry> map = variant.getStudiesMap();
//
//            assertTrue(map.containsKey(studyMetadataMultiFile.getStudyName()));
//            assertTrue(map.containsKey(studyMetadataSingleFile.getStudyName()));
//            assertEquals(map.get(studyMetadataSingleFile.getStudyName()).getSamplesData(), map.get(studyMetadataMultiFile
//                    .getStudyName()).getSamplesData());
//        }
//        assertEquals(expectedNumVariants - 4, numVariants);

    }

    @Test
    public void multiRegionBatchIndex() throws Exception {
        clearDB(DB_NAME);
        StudyMetadata studyMetadata = new StudyMetadata(1, "multiRegion");

        StoragePipelineResult etlResult;
        ObjectMap options = new ObjectMap()
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true)
                .append(VariantStorageOptions.ANNOTATE.key(), false);

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();

        URI chr1 = getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI chr22 = getResourceUri("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

//        runDefaultETL(chr1, this.variantStorageEngine,
//                studyMetadata, options.append(VariantStorageEngine.Options.FILE_ID.key(), 5));
//        Integer defaultCohortId = studyMetadata.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
//        assertTrue(studyMetadata.getCohorts().containsKey(defaultCohortId));
//        assertEquals(2504, studyMetadata.getCohorts().get(defaultCohortId).size());
//        assertTrue(metadataManager.getIndexedFiles(studyMetadata.getId()).contains(5));
//        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyMetadata, true, false, -1);
//
//        runDefaultETL(chr22, this.variantStorageEngine,
////        runDefaultETL(getResourceUri("1k.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"), variantStorageEngine,
//                studyMetadata, options.append(VariantStorageEngine.Options.FILE_ID.key(), 6));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.LoadSplitData.CHROMOSOME);

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
        StudyMetadata studyMetadata = new StudyMetadata(1, "multiRegion");

        ObjectMap options = new ObjectMap()
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true)
                .append(VariantStorageOptions.ANNOTATE.key(), false);

        runDefaultETL(getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
                studyMetadata, options);
        CohortMetadata cohort = metadataManager.getCohortMetadata(studyMetadata.getId(), StudyEntry.DEFAULT_COHORT);
        int fileIdChr1 = metadataManager.getFileId(studyMetadata.getId(), "1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        assertNotNull(cohort);
        assertEquals(2504, cohort.getSamples().size());
        assertTrue(metadataManager.getIndexedFiles(studyMetadata.getId()).contains(fileIdChr1));
        checkLoadedVariants(getVariantStorageEngine().getDBAdaptor(), studyMetadata, true, false, false, -1);

        runDefaultETL(getResourceUri("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
//        runDefaultETL(getResourceUri("1k.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"), variantStorageManager,
                studyMetadata, options
                        .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.LoadSplitData.CHROMOSOME));
        int fileIdChr22 = metadataManager.getFileId(studyMetadata.getId(), "10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");

        assertTrue(metadataManager.getIndexedFiles(studyMetadata.getId()).contains(fileIdChr22));
        checkLoadedVariants(getVariantStorageEngine().getDBAdaptor(), studyMetadata, true, false, false, -1);

        assertEquals(metadataManager.getFileMetadata(studyMetadata.getId(), fileIdChr1).getSamples(),
                metadataManager.getFileMetadata(studyMetadata.getId(), fileIdChr22).getSamples());

        //Check generated stats files
        cohort = metadataManager.getCohortMetadata(studyMetadata.getId(), StudyEntry.DEFAULT_COHORT);
        assertEquals(2504, cohort.getSamples().size());
        File[] statsFile1 = getTmpRootDir().toFile().listFiles((dir, name1) -> name1.startsWith(VariantStoragePipeline.buildFilename(studyMetadata.getName(), fileIdChr1))
                && name1.contains("variants"));
        File[] statsFile2 = getTmpRootDir().toFile().listFiles((dir, name1) -> name1.startsWith(VariantStoragePipeline.buildFilename(studyMetadata.getName(), fileIdChr22))
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
        StudyMetadata studyMetadata = new StudyMetadata(1, "multiRegion");

        ObjectMap options = new ObjectMap()
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true)
                .append(VariantStorageOptions.ANNOTATE.key(), false);

        runDefaultETL(getResourceUri("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
                studyMetadata, options);

        StorageEngineException exception = StorageEngineException.alreadyLoadedSamples(
                "10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
                Arrays.asList("", ""));
        thrown.expect(exception.getClass());
        thrown.expectMessage(exception.getMessage());
        runDefaultETL(getResourceUri("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageEngine,
                studyMetadata, options.append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.LoadSplitData.CHROMOSOME));
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
        StudyMetadata studyMetadata = newStudyMetadata();
        params.put(VariantStorageOptions.STUDY.key(), studyMetadata.getName());
        params.put(VariantStorageOptions.TRANSFORM_FORMAT.key(), "json");
        params.put(VariantStorageOptions.TRANSFORM_COMPRESSION.key(), "gZiP");
        params.put(VariantStorageOptions.TRANSFORM_THREADS.key(), 1);
        params.put(VariantStorageOptions.LOAD_THREADS.key(), 1);
//        params.put(VariantStorageEngine.Options.INCLUDE_GENOTYPES.key(), true);
//        params.put(VariantStorageEngine.Options.INCLUDE_SRC.key(), true);
        StoragePipelineResult etlResult = runETL(variantStorageEngine, params, true, true, true);
        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());

        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.json.gz'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.json.gz"));

        Integer fileId = metadataManager.getFileId(studyMetadata.getId(), inputUri);
        assertTrue(metadataManager.getIndexedFiles(studyMetadata.getId()).contains(fileId));
        VariantFileMetadata fileMetadata = checkTransformedVariants(etlResult.getTransformResult(), studyMetadata);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyMetadata, true, false, getExpectedNumLoadedVariants(fileMetadata));

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
        StudyMetadata studyMetadata = newStudyMetadata();
        params.put(VariantStorageOptions.STUDY.key(), studyMetadata.getName());
        params.put(VariantStorageOptions.TRANSFORM_COMPRESSION.key(), "snappy");
        params.put(VariantStorageOptions.TRANSFORM_THREADS.key(), 8);
        params.put(VariantStorageOptions.LOAD_THREADS.key(), 8);
//        params.put(VariantStorageEngine.Options.INCLUDE_GENOTYPES.key(), false);
//        params.put(VariantStorageEngine.Options.INCLUDE_SRC.key(), false);
        StoragePipelineResult etlResult = runETL(variantStorageEngine, params, true, true, true);

        System.out.println("etlResult = " + etlResult);
        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());

        assertTrue("Incorrect transform file extension " + etlResult.getTransformResult() + ". Expected 'variants.avro.snappy'",
                Paths.get(etlResult.getTransformResult()).toFile().getName().endsWith("variants.avro.snappy"));

        Integer fileId = metadataManager.getFileId(studyMetadata.getId(), inputUri);
        assertTrue(metadataManager.getIndexedFiles(studyMetadata.getId()).contains(fileId));
        VariantFileMetadata fileMetadata = checkTransformedVariants(etlResult.getTransformResult(), studyMetadata);
        checkLoadedVariants(variantStorageEngine.getDBAdaptor(), studyMetadata, false, false, false, getExpectedNumLoadedVariants
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

        StudyMetadata studyMetadata = newStudyMetadata();
        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyMetadata,
                new ObjectMap(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), extraFields)
                        .append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        VariantFileMetadata fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(etlResult.getTransformResult());
        checkTransformedVariants(etlResult.getTransformResult(), studyMetadata, fileMetadata.getStats().getNumVariants());
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        checkLoadedVariants(dbAdaptor, studyMetadata, true, false, false, getExpectedNumLoadedVariants(fileMetadata));

        String fileId = fileName(smallInputUri);
        VariantReader reader = variantReaderUtils.getVariantReader(Paths.get(etlResult.getTransformResult().getPath()),
                new VariantFileMetadata(fileId, "").toVariantStudyMetadata(String.valueOf(studyMetadata.getId())));

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
        StudyMetadata studyMetadata = newStudyMetadata();
        runDefaultETL(smallInputUri, getVariantStorageEngine(), studyMetadata,
                new ObjectMap(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), VariantQueryUtils.NONE)
                        .append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );
        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertEquals("GT", variant.getStudy(STUDY_NAME).getFormatAsString());
        }
    }

    /* ---------------------------------------------------- */
    /* Check methods for loaded and transformed Variants    */
    /* ---------------------------------------------------- */


    private VariantFileMetadata checkTransformedVariants(URI variantsJson, StudyMetadata studyMetadata) throws StorageEngineException {
        return checkTransformedVariants(variantsJson, studyMetadata, -1);
    }

    private VariantFileMetadata checkTransformedVariants(URI variantsJson, StudyMetadata studyMetadata, int expectedNumVariants)
            throws StorageEngineException {
        long start = System.currentTimeMillis();
        VariantFileMetadata source = new VariantFileMetadata("6", VCF_TEST_FILE_NAME);
        VariantReader variantReader = variantReaderUtils.getVariantReader(variantsJson,
                source.toVariantStudyMetadata(String.valueOf(studyMetadata.getId())));

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

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyMetadata studyMetadata, boolean includeSamples, boolean
            includeSrc) {
        checkLoadedVariants(dbAdaptor, studyMetadata, includeSamples, includeSrc, NUM_VARIANTS/*9792*/);
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyMetadata studyMetadata,
                                     boolean includeSamples, boolean includeSrc, int expectedNumVariants) {
        checkLoadedVariants(dbAdaptor, studyMetadata, includeSamples, includeSrc, false, expectedNumVariants);
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyMetadata studyMetadata,
                                     boolean includeSamples, boolean includeSrc, boolean includeAnnotation, int expectedNumVariants) {
        long start = System.currentTimeMillis();
        int numVariants = 0;
        String expectedStudyId = studyMetadata.getName();
        DataResult<Long> count = dbAdaptor.count(new Query());
        assertEquals(1, count.getNumResults());
        if (expectedNumVariants >= 0) {
            assertEquals(expectedNumVariants, count.first().intValue());
        }
//        for (Integer fileId : metadataManager.getIndexedFiles(studyMetadata.getId())) {
//            assertTrue(studyMetadata.getHeaders().containsKey(fileId));
//        }
        Set<String> samples = metadataManager.getIndexedSamplesMap(studyMetadata.getId()).keySet();
        assertEquals(samples.size(), Iterators.size(metadataManager.sampleMetadataIterator(studyMetadata.getId())));
        Map<String, CohortMetadata> cohorts = new HashMap<>();
        metadataManager.cohortIterator(studyMetadata.getId()).forEachRemaining(c -> {
            if (c.isStatsReady()) {
                cohorts.put(c.getName(), c);
            }
        });
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
                    assertEquals(samples.size(), entry.getValue().getSamplesData().size());

                    assertEquals(samples.size(), entry.getValue().getSamplesData().size());
                    assertEquals(new HashSet<>(samples), entry.getValue().getSamplesDataAsMap().keySet());
                }
                for (FileEntry fileEntry : entry.getValue().getFiles()) {
                    if (includeSrc) {
                        assertNotNull(fileEntry.getAttributes().get(VariantVcfFactory.SRC));
                    } else {
                        assertNull(fileEntry.getAttributes().getOrDefault(VariantVcfFactory.SRC, null));
                    }
                }
                for (CohortMetadata cohort : cohorts.values()) {
                    try {
                        VariantStats variantStats = entry.getValue().getStats().get(cohort.getName());
                        assertNotNull(variantStats);
                        assertEquals(variant + " has incorrect stats for cohort \"" + cohort.getName() + "\":"+cohort.getId(),
                                cohort.getSamples().size(),
                                variantStats.getGenotypeCount().values()
                                        .stream()
                                        .mapToInt(Integer::intValue)
                                        .sum());
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
        StudyMetadata studyMetadata = newStudyMetadata();

        params.put(VariantStorageOptions.STUDY.key(), studyMetadata.getName());
        params.put(VariantStorageOptions.TRANSFORM_FORMAT.key(), "json");
        params.put(VariantStorageOptions.TRANSFORM_COMPRESSION.key(), "gZiP");
        params.put(VariantStorageOptions.TRANSFORM_THREADS.key(), 1);
        params.put(VariantStorageOptions.LOAD_THREADS.key(), 1);
        params.put(VariantStorageOptions.ANNOTATE.key(), true);
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
        StudyMetadata studyMetadata1 = variantStorageEngine.getMetadataManager().createStudy("Study1");
        StudyMetadata studyMetadata2 = variantStorageEngine.getMetadataManager().createStudy("Study2");

        ObjectMap options = new ObjectMap(params)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.ANNOTATE.key(), false);
        //Study1
        runDefaultETL(getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyMetadata1, options);
        runDefaultETL(getResourceUri("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyMetadata1, options);

        //Study2
        runDefaultETL(getResourceUri("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyMetadata2, options);
        runDefaultETL(getResourceUri("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyMetadata2, options);
        runDefaultETL(getResourceUri("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"),
                variantStorageEngine, studyMetadata2, options);

        variantStorageEngine.removeFile(studyMetadata1.getName(), 2);

        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertFalse(variant.getStudies().isEmpty());
            StudyEntry study = variant.getStudy("1");
            if (study != null) {
                List<FileEntry> files = study.getFiles();
                assertEquals(1, files.size());
                assertEquals("1", files.get(0).getFileId());
            }
        }

        variantStorageEngine.getDBAdaptor().getMetadataManager().variantFileMetadataIterator(new Query(), new QueryOptions())
                .forEachRemaining(vs -> assertNotEquals("2", vs.getId()));
    }

}
