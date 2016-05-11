package org.opencb.opencga.analysis.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.analysis.storage.OpenCGATestExternalResource.runStorageJob;
import static org.opencb.opencga.analysis.storage.variant.VariantStorageTest.checkCalculatedStats;

/**
 * Created by hpccoll1 on 13/07/15.
 */
public class AnalysisFileIndexerTest extends AbstractAnalysisFileIndexerTest{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Logger logger = LoggerFactory.getLogger(AnalysisFileIndexerTest.class);
    private List<File> files = new ArrayList<>();

    @Before
    public void beforeIndex() throws Exception {
        files.add(create("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
    }

    @Test
    public void testIndexWithStats() throws Exception {
        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false);
        opencga.runStorageJob(analysisFileIndexer.index((int) files.get(0).getId(), (int) outputId, sessionId, queryOptions).first(), sessionId);
        assertEquals(500, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.CohortStatus.NONE, getDefaultCohort().getStatus().getStatus());
        assertNotNull(catalogManager.getFile(files.get(0).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        runStorageJob(catalogManager, analysisFileIndexer.index((int) files.get(1).getId(), (int) outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(1000, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.CohortStatus.NONE, getDefaultCohort().getStatus().getStatus());
        assertNotNull(catalogManager.getFile(files.get(1).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        runStorageJob(catalogManager, analysisFileIndexer.index((int) files.get(2).getId(), (int) outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(1500, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.CohortStatus.READY, getDefaultCohort().getStatus().getStatus());
        checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, catalogManager.getAllCohorts(studyId,
                new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first()),
                catalogManager, dbName, sessionId);
        assertNotNull(catalogManager.getFile(files.get(2).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), false);
        runStorageJob(catalogManager, analysisFileIndexer.index((int) files.get(3).getId(), (int) outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(2000, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.CohortStatus.INVALID, getDefaultCohort().getStatus().getStatus());
        assertNotNull(catalogManager.getFile(files.get(3).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        runStorageJob(catalogManager, analysisFileIndexer.index((int) files.get(4).getId(), (int) outputId, sessionId, queryOptions).first(), logger, sessionId);
        assertEquals(2504, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.CohortStatus.READY, getDefaultCohort().getStatus().getStatus());
        assertNotNull(catalogManager.getFile(files.get(4).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, catalogManager.getAllCohorts(studyId,
                new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first()),
                catalogManager, dbName, sessionId);
    }

    @Test
    public void testIndexBySteps() throws Exception {
        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false).append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);

        queryOptions.append(AnalysisFileIndexer.TRANSFORM, true);
        queryOptions.append(AnalysisFileIndexer.LOAD, false);

        //Create transform index job
        Job job = analysisFileIndexer.index((int) files.get(0).getId(), (int) outputId, sessionId, queryOptions).first();
        assertEquals(Index.IndexStatus.TRANSFORMING, catalogManager.getFile(files.get(0).getId(), sessionId).first().getIndex().getStatus().getStatus());

        //Run transform index job
        job = runStorageJob(catalogManager, job, logger, sessionId);
        assertEquals(500, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.CohortStatus.NONE, getDefaultCohort().getStatus().getStatus());
        assertEquals(Index.IndexStatus.TRANSFORMED, catalogManager.getFile(files.get(0).getId(), sessionId).first().getIndex().getStatus().getStatus());
        assertEquals(job.getAttributes().get(Job.TYPE), Job.Type.INDEX.toString());

        //Get transformed file
        Query searchQuery = new Query(CatalogFileDBAdaptor.QueryParams.ID.key(), job.getOutput())
                .append(CatalogFileDBAdaptor.QueryParams.NAME.key(), "~variants.(json|avro)");
        File transformedFile = catalogManager.getAllFiles(studyId, searchQuery, new QueryOptions(), sessionId).first();
        assertEquals(job.getId(), transformedFile.getJobId());
        File file = catalogManager.getFile(files.get(0).getId(), sessionId).first();
        assertNotNull(file.getStats().get(FileMetadataReader.VARIANT_STATS));

        //Create load index job
        queryOptions.append(AnalysisFileIndexer.TRANSFORM, false);
        queryOptions.append(AnalysisFileIndexer.LOAD, true);
        job = analysisFileIndexer.index((int) transformedFile.getId(), (int) outputId, sessionId, queryOptions).first();
        assertEquals(Index.IndexStatus.LOADING, catalogManager.getFile(files.get(0).getId(), sessionId).first().getIndex().getStatus().getStatus());
        assertEquals(job.getAttributes().get(Job.TYPE), Job.Type.INDEX.toString());

        //Run load index job
        job = runStorageJob(catalogManager, job, logger, sessionId);
        assertEquals(job.getStatus().getStatus(), Status.READY);
        assertEquals(500, getDefaultCohort().getSamples().size());
        assertEquals(Cohort.CohortStatus.READY, getDefaultCohort().getStatus().getStatus());
        assertEquals(Index.IndexStatus.READY, catalogManager.getFile(files.get(0).getId(), sessionId).first().getIndex().getStatus().getStatus());
        Cohort defaultCohort = catalogManager.getAllCohorts(studyId,
                new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first();
        checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, defaultCohort), catalogManager, dbName, sessionId);
    }


}