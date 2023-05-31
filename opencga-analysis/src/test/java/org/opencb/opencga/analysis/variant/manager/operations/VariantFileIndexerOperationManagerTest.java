/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.file.FileDeleteTask;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternalVariantIndex;
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.analysis.variant.manager.operations.StatsVariantStorageTest.checkCalculatedStats;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created by hpccoll1 on 13/07/15.
 */
@Category(MediumTests.class)
public class VariantFileIndexerOperationManagerTest extends AbstractVariantOperationManagerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Logger logger = LoggerFactory.getLogger(VariantFileIndexerOperationManagerTest.class);

    @Test
    public void testIndexWithStats() throws Exception {

        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false);
        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), false);

        variantManager.index(studyId, getFile(0).getId(), newTmpOutdir(), queryOptions, sessionId);
        assertEquals(500, getDefaultCohort(studyId).getSamples().size());
        assertEquals(CohortStatus.NONE, getDefaultCohort(studyId).getInternal().getStatus().getId());
        VariantSetStats stats = getVariantSetMetrics(getFile(0).getId());
        assertNotEquals(0, stats.getVariantCount().intValue());

        variantManager.index(studyId, getFile(1).getId(), newTmpOutdir(), queryOptions, sessionId);
        assertEquals(1000, getDefaultCohort(studyId).getSamples().size());
        assertEquals(CohortStatus.NONE, getDefaultCohort(studyId).getInternal().getStatus().getId());
        assertNotNull(getVariantSetMetrics(getFile(1).getId()));

        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), true);
        variantManager.index(studyId, getFile(2).getId(), newTmpOutdir(), queryOptions, sessionId);
        assertEquals(1500, getDefaultCohort(studyId).getSamples().size());
        assertEquals(CohortStatus.READY, getDefaultCohort(studyId).getInternal().getStatus().getId());
        checkCalculatedStats(studyId, Collections.singletonMap(DEFAULT_COHORT, catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first()), catalogManager,
                dbName, sessionId);
        assertNotNull(getVariantSetMetrics(getFile(2).getId()));

        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), false);
        variantManager.index(studyId, getFile(3).getId(), newTmpOutdir(), queryOptions, sessionId);
        assertEquals(2000, getDefaultCohort(studyId).getSamples().size());
        assertEquals(CohortStatus.INVALID, getDefaultCohort(studyId).getInternal().getStatus().getId());
        assertNotNull(getVariantSetMetrics(getFile(3).getId()));

        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), true);
        variantManager.index(studyId, getFile(4).getId(), newTmpOutdir(), queryOptions, sessionId);
        assertEquals(2504, getDefaultCohort(studyId).getSamples().size());
        assertEquals(CohortStatus.READY, getDefaultCohort(studyId).getInternal().getStatus().getId());
        assertNotNull(getVariantSetMetrics(getFile(4).getId()));
        checkCalculatedStats(studyId, Collections.singletonMap(DEFAULT_COHORT, catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first()), catalogManager,
                dbName, sessionId);
    }

    private VariantSetStats getVariantSetMetrics(String fileId) throws CatalogException {
        return catalogManager.getFileManager().get(studyId, fileId, null, sessionId).first().getQualityControl().getVariant().getVariantSetMetrics();
    }

    @Test
    public void testIndexWithStatsLowerCaseAggregationType() throws Exception {

        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false);
        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), true);
        queryOptions.put(VariantStorageOptions.STATS_AGGREGATION.key(), "none");

        variantManager.index(studyId, getFile(0).getId(), newTmpOutdir(), queryOptions, sessionId);
        assertEquals(500, getDefaultCohort(studyId).getSamples().size());
        assertEquals(CohortStatus.READY, getDefaultCohort(studyId).getInternal().getStatus().getId());
        assertNotNull(getVariantSetMetrics(getFile(0).getId()));

    }

    @Test
    public void testIndexWithStatsWrongAggregationType() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false);
        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), true);
        queryOptions.put(VariantStorageOptions.STATS_AGGREGATION.key(), "wrong_type");

        try {
            variantManager.index(studyId, getFile(0).getId(), newTmpOutdir(), queryOptions, sessionId);
            fail("Expected StoragePipelineException exception");
        } catch (Exception e) {
            assertEquals(0, getDefaultCohort(studyId).getSamples().size());
            assertEquals(CohortStatus.NONE, getDefaultCohort(studyId).getInternal().getStatus().getId());
            assertEquals(VariantIndexStatus.NONE, catalogManager.getFileManager().get(studyId, getFile(0).getId(), null, sessionId).first().getInternal().getVariant().getIndex().getStatus().getId());
            assertNull(getVariantSetMetrics(getFile(0).getId()));
        }
        queryOptions.put(VariantStorageOptions.STATS_AGGREGATION.key(), "none");
        variantManager.index(studyId, getFile(0).getId(), newTmpOutdir(), queryOptions, sessionId);
        assertEquals(500, getDefaultCohort(studyId).getSamples().size());
        assertEquals(CohortStatus.READY, getDefaultCohort(studyId).getInternal().getStatus().getId());
        assertNotNull(getVariantSetMetrics(getFile(0).getId()));

    }

    String newTmpOutdir() throws CatalogException, IOException {
        return opencga.createTmpOutdir(studyId, "index", sessionId);
    }

    @Test
    public void testIndex() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        indexFile(getFile(0), queryOptions, outputId);
    }

    @Test
    public void testDeleteIndexedFile() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        File inputFile = getFile(0);
        indexFile(inputFile, queryOptions, outputId);
        Study study = catalogManager.getFileManager().getStudy(inputFile, sessionId);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("The status is READY");
        catalogManager.getFileManager().unlink(study.getFqn(), inputFile.getId(), sessionId);
    }

    @Test
    public void testDeleteSampleFromIndexedFile() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        File inputFile = getFile(0);
        indexFile(inputFile, queryOptions, outputId);
        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), inputFile.getSampleIds().get(100));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Sample associated to the files");
        DataResult delete = catalogManager.getSampleManager().delete(studyFqn, query, null, sessionId);
    }

    @Test
    public void testIndexFromFolder() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        File file = getFile(0);
        Path pathParent = Paths.get(file.getPath()).getParent();

        File parent = catalogManager.getFileManager().search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), pathParent.toString() + "/"), null, sessionId).first();
        indexFiles(singletonList(parent), singletonList(file), queryOptions, outputId);
    }

    @Test
    public void testIndexBySteps() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        File transformedFile = transformFile(getFile(0), queryOptions);
        loadFile(transformedFile, queryOptions, outputId);
    }

    @Ignore
    @Test
    public void testDeleteTransformedFile() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        File inputFile = getFile(0);
        File transformedFile = transformFile(inputFile, queryOptions);

        catalogManager.getFileManager().delete(studyFqn,
                new Query(FileDBAdaptor.QueryParams.NAME.key(), transformedFile.getName()), new QueryOptions(Constants.SKIP_TRASH, true),
                sessionId);
        catalogManager.getFileManager().delete(studyFqn, new Query(FileDBAdaptor.QueryParams.NAME.key(),
                        VariantReaderUtils.getMetaFromTransformedFile(transformedFile.getName())),
                new QueryOptions(Constants.SKIP_TRASH, true), sessionId);

        indexFile(inputFile, queryOptions, outputId);
    }

    @Test
    public void testIndexByStepsWithStats() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true);

        File transformedFile = transformFile(getFile(0), queryOptions);
        loadFile(transformedFile, queryOptions, outputId);
    }

    @Test
    public void testDeleteCohortWithStats() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true);

        File transformedFile = transformFile(getFile(0), queryOptions);
        loadFile(transformedFile, queryOptions, outputId);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("ALL cannot be deleted");
        catalogManager.getCohortManager().delete(studyFqn, new Query(CohortDBAdaptor.QueryParams.ID.key(), "ALL"), null, sessionId);
    }

    @Test
    public void testIndexByStepsSameInput() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true);

        transformFile(getFile(0), queryOptions);
        loadFile(getFile(0), queryOptions, outputId);

    }

    @Test
    public void testIndexWithTransformError() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        DummyVariantStoragePipeline storageETL = mockVariantStorageETL();
        List<File> files = Arrays.asList(getFile(0), getFile(1));
        StorageEngineException transformException = StorageEngineException.unableToExecute("transform", 0, "");
        Mockito.doThrow(transformException).when(storageETL)
                .transform(ArgumentMatchers.argThat(argument -> argument.toString().contains(files.get(1).getName())), Mockito.any(), Mockito.any());

        try {
            indexFiles(files, queryOptions, outputId);
            fail();
        } catch (StoragePipelineException e) {
//            StoragePipelineException exception = (StoragePipelineException) e.getCause();
            StoragePipelineException exception = e;
            assertEquals(files.size(), exception.getResults().size());

            assertTrue(exception.getResults().get(0).isTransformExecuted());
            assertNull(exception.getResults().get(0).getTransformError());

            assertTrue(exception.getResults().get(1).isTransformExecuted());
            assertSame(transformException, exception.getResults().get(1).getTransformError());

            for (int i = files.size(); i > 0; i--) {
                assertFalse(exception.getResults().get(1).isLoadExecuted());
                assertNull(exception.getResults().get(1).getLoadError());
            }

        }

        mockVariantStorageETL();
        queryOptions.append(VariantFileIndexerOperationManager.SKIP_INDEXED_FILES, true);
        // File 0 already transformed.
        // Expecting to transform and load only file 1
        indexFiles(files, singletonList(files.get(1)), queryOptions, outputId);
    }

    @Test
    public void testTransformTransformingFiles() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantFileIndexerOperationManager.SKIP_INDEXED_FILES, true);

        List<File> files = Arrays.asList(getFile(0), getFile(1));
        catalogManager.getFileManager().updateFileInternalVariantIndex(getFile(1),
                FileInternalVariantIndex.init().setStatus(new VariantIndexStatus(VariantIndexStatus.TRANSFORMING)), sessionId);

        // Expect both files to be loaded
        indexFiles(files, Arrays.asList(getFile(0)), queryOptions, outputId);
    }

    @Test
    public void testResumeTransformTransformingFiles() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.RESUME.key(), true);

        List<File> files = Arrays.asList(getFile(0), getFile(1));
        catalogManager.getFileManager().updateFileInternalVariantIndex(getFile(1), new FileInternalVariantIndex(new VariantIndexStatus(VariantIndexStatus.TRANSFORMING), 0, null), sessionId);

        // Expect only the first file to be loaded
        indexFiles(files, files, queryOptions, outputId);
    }

    @Test
    public void testIndexWithLoadErrorExternalOutputFolder() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantFileIndexerOperationManager.SKIP_INDEXED_FILES, true);

        List<File> files = Arrays.asList(getFile(0), getFile(1));
        queryOptions.put(DummyVariantStoragePipeline.VARIANTS_LOAD_FAIL, files.get(1).getName() + ".variants.avro.gz");
        List<String> fileIds = files.stream().map(File::getId).collect(Collectors.toList());
        try {
            String outdir = opencga.createTmpOutdir(studyId, "_INDEX", sessionId);
            variantManager.index(studyId, fileIds, outdir, queryOptions, sessionId);
            fail();
        } catch (StorageEngineException e) {
            e.printStackTrace();
//            StoragePipelineException exception = (StoragePipelineException) e.getCause();
            StoragePipelineException exception = (StoragePipelineException) e;
            assertEquals(files.size(), exception.getResults().size());

            for (int i = 0; i < files.size(); i++) {
                assertTrue(exception.getResults().get(i).isTransformExecuted());
                assertNull(exception.getResults().get(i).getTransformError());
            }

            assertTrue(exception.getResults().get(0).isLoadExecuted());
            assertNull(exception.getResults().get(0).getLoadError());

            assertTrue(exception.getResults().get(1).isLoadExecuted());
        }
        queryOptions.put(DummyVariantStoragePipeline.VARIANTS_LOAD_FAIL, false);
        indexFiles(files, singletonList(files.get(1)), queryOptions, outputId);
    }

    @Test
    public void testIndexWithLoadError() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        List<File> files = Arrays.asList(getFile(0), getFile(1));
        queryOptions.put(DummyVariantStoragePipeline.VARIANTS_LOAD_FAIL, files.get(1).getName() + ".variants.avro.gz");
        try {
            indexFiles(files, queryOptions, outputId);
            fail();
        } catch (StoragePipelineException e) {
//            StoragePipelineException exception = (StoragePipelineException) e.getCause();
            StoragePipelineException exception = e;
            assertEquals(files.size(), exception.getResults().size());

            for (int i = 0; i < files.size(); i++) {
                assertTrue(exception.getResults().get(i).isTransformExecuted());
                assertNull(exception.getResults().get(i).getTransformError());
            }

            assertTrue(exception.getResults().get(0).isLoadExecuted());
            assertNull(exception.getResults().get(0).getLoadError());

            assertTrue(exception.getResults().get(1).isLoadExecuted());
        }
        queryOptions.put(DummyVariantStoragePipeline.VARIANTS_LOAD_FAIL, false);
        // File 0 already loaded.
        // Expecting to load only file 1
        loadFiles(files, singletonList(files.get(1)), queryOptions, outputId);
    }

    @Test
    public void testIndexByStepsExternallyTransformed() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantFileIndexerOperationManager.TRANSFORM, true)
                // TODO: Should work without isolating transformation?
                .append(VariantStorageOptions.TRANSFORM_ISOLATE.key(), true);

//        File transformFile = transformFile(getFile(0), queryOptions);
        String outdir = opencga.createTmpOutdir(studyId, "_TRANSFORM_", sessionId);
        List<StoragePipelineResult> etlResults = variantManager.index(studyId, getFile(0).getPath(),
                outdir, queryOptions, sessionId);

        File transformFile = null;
        create(studyId2, catalogManager.getFileManager().getUri(getFile(0)));
        for (java.io.File file : Paths.get(UriUtils.createUri(outdir)).toFile().listFiles()) {
            File f = create(studyId2, file.toURI());
            if (VariantReaderUtils.isTransformedVariants(file.toString())) {
                assertNull(transformFile);
                transformFile = f;
            }
        }
        assertNotNull(transformFile);
        catalogManager.getFileManager().matchUpVariantFiles(studyId2, singletonList(transformFile), sessionId);

        queryOptions = new QueryOptions().append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true);
        loadFile(transformFile, queryOptions, outputId2);

    }

    @Test
    public void testIndexMalformed() throws Exception {
        ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantManager.getStorageConfiguration()));

        Path outDir = Paths.get(opencga.createTmpOutdir("_malformed_file"));
        VariantIndexParams params = new VariantIndexParams();
        params.setFile(create("variant-test-file-corrupted.vcf").getName());

        ExecutionResult er = toolRunner.execute(VariantIndexOperationTool.class, params.toObjectMap()
                        .append(ParamConstants.STUDY_PARAM, studyId)
                        .append(VariantStorageOptions.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), false)
                , outDir, null, sessionId);

        assertEquals(Event.Type.WARNING, er.getEvents().get(0).getType());
        assertThat(er.getEvents().get(0).getMessage(), CoreMatchers.containsString("Found malformed variants"));
        assertTrue(Files.exists(outDir.resolve("variant-test-file-corrupted.vcf.malformed.txt")));
    }

    @Test
    public void testIndexDuplicated() throws Exception {
        ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantManager.getStorageConfiguration()));

        Path outDir = Paths.get(opencga.createTmpOutdir("_duplicated_file"));
        VariantIndexParams params = new VariantIndexParams();
        params.setFile(create("variant-test-duplicated.vcf").getName());

        ExecutionResult er = toolRunner.execute(VariantIndexOperationTool.class, params.toObjectMap()
                        .append(ParamConstants.STUDY_PARAM, studyId)
                        .append(VariantStorageOptions.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), false)
                , outDir, null, sessionId);

        assertEquals(Event.Type.WARNING, er.getEvents().get(0).getType());
        assertThat(er.getEvents().get(0).getMessage(), CoreMatchers.containsString("Found duplicated variants"));
        assertTrue(Files.exists(outDir.resolve("variant-test-duplicated.vcf.duplicated.tsv")));
    }

    @Test
    public void testIndexWeirdFileName() throws Exception {
        ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantManager.getStorageConfiguration()));

        Path outDir = Paths.get(opencga.createTmpOutdir("_escaped_name"));

        Path filePath = Paths.get(getResourceUri("variant-test-file.vcf.gz"));
        filePath = Files.move(filePath, filePath.getParent().resolve("variant-test%3Afile.vcf.gz"));

        VariantIndexParams params = new VariantIndexParams();
        params.setFile(create(studyId, filePath.toUri()).getName());

        ExecutionResult er = toolRunner.execute(VariantIndexOperationTool.class, params.toObjectMap()
                        .append(ParamConstants.STUDY_PARAM, studyId)
                , outDir, null, sessionId);
    }

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }
}