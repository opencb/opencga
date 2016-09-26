package org.opencb.opencga.analysis.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.analysis.storage.OpenCGATestExternalResource.runStorageJob;
import static org.opencb.opencga.analysis.storage.variant.StatsVariantStorageTest.checkCalculatedStats;

/**
 * Created by hpccoll1 on 13/07/15.
 */
public class AnalysisFileIndexerTest extends AbstractAnalysisFileIndexerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Logger logger = LoggerFactory.getLogger(AnalysisFileIndexerTest.class);
    private List<File> files = new ArrayList<>();
    private AnalysisFileIndexer analysisFileIndexer;

    @Before
    public void beforeIndex() throws Exception {
        files.add(create("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
    }

    @Test
    public void testIndexWithStats() throws Exception {
        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false);
        opencga.runStorageJob(analysisFileIndexer.index(files.get(0).getId(), outputId, sessionId, queryOptions).first(), sessionId);
        assertEquals(500, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.NONE, getDefaultCohort(studyId).getStatus().getName());
        assertNotNull(catalogManager.getFile(files.get(0).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(1).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(1000, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.NONE, getDefaultCohort(studyId).getStatus().getName());
        assertNotNull(catalogManager.getFile(files.get(1).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(2).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(1500, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.READY, getDefaultCohort(studyId).getStatus().getName());
        checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, catalogManager.getAllCohorts(studyId,
                new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first()),
                catalogManager, dbName, sessionId);
        assertNotNull(catalogManager.getFile(files.get(2).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), false);
        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(3).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(2000, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.INVALID, getDefaultCohort(studyId).getStatus().getName());
        assertNotNull(catalogManager.getFile(files.get(3).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        runStorageJob(catalogManager, analysisFileIndexer.index(files.get(4).getId(), outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(2504, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.READY, getDefaultCohort(studyId).getStatus().getName());
        assertNotNull(catalogManager.getFile(files.get(4).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, catalogManager.getAllCohorts(studyId,
                new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first()),
                catalogManager, dbName, sessionId);
    }

    @Test
    public void testIndexBySteps() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);

        File transformedFile = transformFile(files.get(0), queryOptions);
        loadFile(transformedFile, queryOptions, outputId);
    }

    @Test
    public void testIndexByStepsSameInput() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);

        transformFile(files.get(0), queryOptions);
        loadFile(files.get(0), queryOptions, outputId);

    }

    @Test
    public void testIndexByStepsExternallyTransformed() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);

        File transformFile = transformFile(files.get(0), queryOptions);
        Query searchQuery = new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), transformFile.getJobId())
                .append(FileDBAdaptor.QueryParams.NAME.key(), "~file.(json|avro)");
        File transformSourceFile = catalogManager.getAllFiles(studyId, searchQuery, new QueryOptions(), sessionId).first();

        create(studyId2, catalogManager.getFileUri(files.get(0)));
        create(studyId2, catalogManager.getFileUri(transformSourceFile));
        transformFile = create(studyId2, catalogManager.getFileUri(transformFile));

        loadFile(transformFile, queryOptions, outputId2);

    }

    public File transformFile(File inputFile, QueryOptions queryOptions) throws CatalogException, AnalysisExecutionException, IOException {

        queryOptions.append(AnalysisFileIndexer.TRANSFORM, true);
        queryOptions.append(AnalysisFileIndexer.LOAD, false);
        boolean calculateStats = queryOptions.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key());

        //Create transform index job
        Job job = analysisFileIndexer.index(inputFile.getId(), outputId, sessionId, queryOptions).first();
        assertEquals(FileIndex.IndexStatus.TRANSFORMING, catalogManager.getFile(inputFile.getId(), sessionId).first().getIndex().getStatus().getName());

        //Run transform index job
        job = runStorageJob(catalogManager, job, logger, sessionId);

        Cohort defaultCohort = getDefaultCohort(catalogManager.getStudyIdByFileId(inputFile.getId()));
        assertEquals(500, defaultCohort.getSamples().size());
        assertEquals(Cohort.CohortStatus.NONE, defaultCohort.getStatus().getName());
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, catalogManager.getFile(inputFile.getId(), sessionId).first().getIndex().getStatus().getName());

        assertEquals(job.getAttributes().get(Job.TYPE), Job.Type.INDEX.toString());

        //Get transformed file
        Query searchQuery = new Query(FileDBAdaptor.QueryParams.ID.key(), job.getOutput())
                .append(FileDBAdaptor.QueryParams.NAME.key(), "~variants.(json|avro)");
        File transformedFile = catalogManager.getAllFiles(studyId, searchQuery, new QueryOptions(), sessionId).first();
        assertEquals(job.getId(), transformedFile.getJobId());
        inputFile = catalogManager.getFile(inputFile.getId(), sessionId).first();
        assertNotNull(inputFile.getStats().get(FileMetadataReader.VARIANT_STATS));
        return transformedFile;
    }

    public void loadFile(File transformedFile, QueryOptions queryOptions, long outputId) throws Exception {

        Job job;//Create load index job
        queryOptions.append(AnalysisFileIndexer.TRANSFORM, false);
        queryOptions.append(AnalysisFileIndexer.LOAD, true);
        boolean calculateStats = queryOptions.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key());


        job = analysisFileIndexer.index(transformedFile.getId(), outputId, sessionId, queryOptions).first();
        long indexedFileId = ((Number) job.getAttributes().get(Job.INDEXED_FILE_ID)).longValue();
        assertEquals(FileIndex.IndexStatus.LOADING, catalogManager.getFile(indexedFileId, sessionId).first().getIndex().getStatus().getName());
        assertEquals(job.getAttributes().get(Job.TYPE), Job.Type.INDEX.toString());

        //Run load index job
        job = runStorageJob(catalogManager, job, logger, sessionId);
        assertEquals(Status.READY, job.getStatus().getName());
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(indexedFileId, sessionId).first().getIndex().getStatus().getName());

        Cohort defaultCohort = getDefaultCohort(catalogManager.getStudyIdByFileId(indexedFileId));
        assertEquals(500, defaultCohort.getSamples().size());
        if (calculateStats) {
            assertEquals(Cohort.CohortStatus.READY, defaultCohort.getStatus().getName());
            checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, defaultCohort), catalogManager, dbName, sessionId);
        }
    }


    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }
}