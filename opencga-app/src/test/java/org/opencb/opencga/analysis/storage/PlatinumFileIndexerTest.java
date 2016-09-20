package org.opencb.opencga.analysis.storage;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.storage.variant.VariantFetcher;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileIndex;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.opencb.opencga.analysis.storage.OpenCGATestExternalResource.runStorageJob;

/**
 * Created on 15/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PlatinumFileIndexerTest extends AbstractAnalysisFileIndexerTest {

    private AnalysisFileIndexer analysisFileIndexer;
    private Logger logger = LoggerFactory.getLogger(AbstractAnalysisFileIndexerTest.class);

    @Override
    protected String getStorageEngine() {
        return STORAGE_ENGINE_HADOOP;
    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }

    @Before
    public void before() {
        analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
    }

    @Test
    public void test() throws Exception {

        File inputFile;
        File transformFile;
        for (int i = 77; i <= 93; i++) {
            inputFile = create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz");
            transformFile = transformFile(inputFile, new QueryOptions());
            loadFile(transformFile, new QueryOptions());
        }

        VariantFetcher fetcher = new VariantFetcher(catalogManager, StorageManagerFactory.get());
        fetcher.iterator(new Query(VariantQueryParams.STUDIES.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
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
        assertEquals(Status.READY, job.getStatus().getName());
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, catalogManager.getFile(inputFile.getId(), sessionId).first().getIndex().getStatus().getName());

        //Get transformed file
        Query searchQuery = new Query(FileDBAdaptor.QueryParams.ID.key(), job.getOutput())
                .append(FileDBAdaptor.QueryParams.NAME.key(), "~variants.(json|avro)");
        File transformedFile = catalogManager.getAllFiles(studyId, searchQuery, new QueryOptions(), sessionId).first();
        assertEquals(job.getId(), transformedFile.getJobId());
        inputFile = catalogManager.getFile(inputFile.getId(), sessionId).first();
        assertNotNull(inputFile.getStats().get(FileMetadataReader.VARIANT_STATS));
        return transformedFile;
    }

    public Job loadFile(File inputFile, QueryOptions queryOptions) throws CatalogException, AnalysisExecutionException, IOException {
        queryOptions.append(AnalysisFileIndexer.TRANSFORM, false);
        queryOptions.append(AnalysisFileIndexer.LOAD, true);
        boolean calculateStats = queryOptions.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key());

        //Create transform index job
        Job job = analysisFileIndexer.index(inputFile.getId(), outputId, sessionId, queryOptions).first();
        long indexedFileId = ((Number) job.getAttributes().get(Job.INDEXED_FILE_ID)).longValue();
        assertEquals(FileIndex.IndexStatus.LOADING, catalogManager.getFile(indexedFileId, sessionId).first().getIndex().getStatus().getName());
        assertEquals(job.getAttributes().get(Job.TYPE), Job.Type.INDEX.toString());

        //Run transform index job
        job = runStorageJob(catalogManager, job, logger, sessionId);
        assertEquals(Status.READY, job.getStatus().getName());
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(indexedFileId, sessionId).first().getIndex().getStatus().getName());

        return job;
    }

}
