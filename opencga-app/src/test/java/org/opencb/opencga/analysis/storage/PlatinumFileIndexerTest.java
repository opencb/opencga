package org.opencb.opencga.analysis.storage;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.storage.variant.VariantFetcher;
import org.opencb.opencga.analysis.variant.VariantFileIndexer;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileIndex;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created on 15/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PlatinumFileIndexerTest extends AbstractAnalysisFileIndexerTest {

    private VariantFileIndexer fileIndexer;
    private Logger logger = LoggerFactory.getLogger(AbstractAnalysisFileIndexerTest.class);

    @Override
    protected String getStorageEngine() {
        return STORAGE_ENGINE_MONGODB;
    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }

    @Before
    public void before() throws CatalogException {
        fileIndexer = new VariantFileIndexer(catalogManager.getCatalogConfiguration(), opencga.getStorageConfiguration());
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

    @Test
    public void testBatch() throws Exception {

        File inputFile;
        List<File> files = new ArrayList<>();
        for (int i = 77; i <= 93; i++) {
            inputFile = create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz");
            files.add(transformFile(inputFile, new QueryOptions()));
        }
        loadFiles(files, new QueryOptions());

        VariantFetcher fetcher = new VariantFetcher(catalogManager, StorageManagerFactory.get());
        fetcher.iterator(new Query(VariantQueryParams.STUDIES.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

    public File transformFile(File inputFile, QueryOptions queryOptions) throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageManagerException, URISyntaxException, InstantiationException, IllegalAccessException {
        queryOptions.append(VariantFileIndexer.TRANSFORM, true);
        queryOptions.append(VariantFileIndexer.LOAD, false);
        queryOptions.append(VariantFileIndexer.CATALOG_PATH, outputId);
        boolean calculateStats = queryOptions.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key());

        long studyId = catalogManager.getStudyIdByFileId(inputFile.getId());

        //Create transform index job
//        Job job = fileIndexer.index(inputFile.getId(), outputId, sessionId, queryOptions).first();
        String tmpOutdir = opencga.createTmpOutdir(studyId, "_TRANSFORM_" + inputFile.getId(), sessionId);
        List<StorageETLResult> results = fileIndexer.index(Collections.singletonList(inputFile.getId()), tmpOutdir, sessionId, queryOptions);
        // TODO: Check TRANSFORMING status!
//        assertEquals(FileIndex.IndexStatus.TRANSFORMING, catalogManager.getFile(inputFile.getId(), sessionId).first().getIndex().getStatus().getName());

//        //Run transform index job
//        job = runStorageJob(catalogManager, job, logger, sessionId);
//        assertEquals(Status.READY, job.getStatus().getName());
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, catalogManager.getFile(inputFile.getId(), sessionId).first().getIndex().getStatus().getName());

        //Get transformed file
        String transformFileName = Paths.get(results.get(0).getTransformResult().getPath()).getFileName().toString();
        Query searchQuery = new Query(FileDBAdaptor.QueryParams.NAME.key(), transformFileName);
        File transformedFile = catalogManager.getAllFiles(studyId, searchQuery, new QueryOptions(), sessionId).first();
//        assertEquals(job.getId(), transformedFile.getJobId());
        inputFile = catalogManager.getFile(inputFile.getId(), sessionId).first();
        assertNotNull(inputFile.getStats().get(FileMetadataReader.VARIANT_STATS));
        return transformedFile;
    }

    public StorageETLResult loadFile(File inputFile, QueryOptions queryOptions) throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageManagerException, URISyntaxException, InstantiationException, IllegalAccessException {
        return loadFiles(Collections.singletonList(inputFile), queryOptions).get(0);
    }

    public List<StorageETLResult> loadFiles(List<File> inputFiles, QueryOptions queryOptions) throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageManagerException, URISyntaxException, InstantiationException, IllegalAccessException {
        queryOptions.append(VariantFileIndexer.TRANSFORM, false);
        queryOptions.append(VariantFileIndexer.LOAD, true);
        queryOptions.append(VariantFileIndexer.CATALOG_PATH, outputId);
        boolean calculateStats = queryOptions.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key());

        List<Long> ids = inputFiles.stream().map(File::getId).collect(Collectors.toList());
        String idsStr = inputFiles.stream().map((file) -> String.valueOf(file.getId())).collect(Collectors.joining("_"));
        //Create transform index job
        String tmpOutdir = opencga.createTmpOutdir(studyId, "_LOAD_" + idsStr, sessionId);
        List<StorageETLResult> results = fileIndexer.index(ids, tmpOutdir, sessionId, queryOptions);

        //TODO: Check loading status!
//        long indexedFileId = ((Number) job.getAttributes().get(Job.INDEXED_FILE_ID)).longValue();
//        assertEquals(FileIndex.IndexStatus.LOADING, catalogManager.getFile(indexedFileId, sessionId).first().getIndex().getStatus().getName());
//        assertEquals(job.getAttributes().get(Job.TYPE), Job.Type.INDEX.toString());

//        //Run transform index job
//        job = runStorageJob(catalogManager, job, logger, sessionId);
//        assertEquals(Status.READY, job.getStatus().getName());

//        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(indexedFileId, sessionId).first().getIndex().getStatus().getName());

        return results;
    }

}
