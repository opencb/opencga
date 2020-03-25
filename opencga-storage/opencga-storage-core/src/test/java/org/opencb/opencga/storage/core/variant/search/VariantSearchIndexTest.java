package org.opencb.opencga.storage.core.variant.search;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantSearchIndexTest extends VariantStorageBaseTest {

    @Rule
    public VariantSolrExternalResource solr = new VariantSolrExternalResource();

    @Before
    public void setUp() throws Exception {
        solr.configure(variantStorageEngine);
    }

    @Test
    public void testIncrementalIndex() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        VariantStorageEngine storageEngine = getVariantStorageEngine();
        QueryOptions options = new QueryOptions();

        int maxStudies = 2;
        int studyCounter = 1;
        int release = 1;
        List<URI> inputFiles = new ArrayList<>();
        List<String> fileNames = new ArrayList<>();
        StudyMetadata studyMetadata = metadataManager.createStudy("S_" + studyCounter);
        int studyId = studyMetadata.getId();
        for (int fileId = 12877; fileId <= 12893; fileId++) {
            String fileName = "1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            URI inputFile = getResourceUri("platinum/" + fileName);
            fileNames.add(fileName);
            inputFiles.add(inputFile);
            metadataManager.registerFile(studyMetadata.getId(), fileName, Arrays.asList("NA" + fileId));
            if (inputFiles.size() == 4) {
//                dbAdaptor.getMetadataManager().updateStudyMetadata(studyMetadata, null);
                options.put(VariantStorageOptions.STUDY.key(), studyId);
                storageEngine.getOptions().putAll(options);
                storageEngine.getOptions().put(VariantStorageOptions.RELEASE.key(), release++);
                storageEngine.index(inputFiles.subList(0, 2), outputUri, true, true, true);

                Query query = new Query(VariantQueryParam.STUDY.key(), studyId);
                long expected = dbAdaptor.count(query).first();

                VariantSearchLoadResult loadResult = searchIndex();
                System.out.println("Load result after load 1,2 files: = " + loadResult + ", at study : " + studyId);
                checkLoadResult(expected, loadResult);
                checkVariantSearchIndex(dbAdaptor);

                //////////////////////
                storageEngine.getOptions().putAll(options);
                storageEngine.getOptions().put(VariantStorageOptions.RELEASE.key(), release++);
                storageEngine.index(inputFiles.subList(2, 4), outputUri, true, true, true);

                expected = dbAdaptor.count(query.append(VariantQueryParam.FILE.key(),
                        "!" + fileNames.get(0) + ";!" + fileNames.get(1) + ";" + fileNames.get(2) + ";" + fileNames.get(3))).first();
                expected += dbAdaptor.count(query.append(VariantQueryParam.FILE.key(),
                        "!" + fileNames.get(0) + ";!" + fileNames.get(1) + ";!" + fileNames.get(2) + ";" + fileNames.get(3))).first();
                expected += dbAdaptor.count(query.append(VariantQueryParam.FILE.key(),
                        "!" + fileNames.get(0) + ";!" + fileNames.get(1) + ";" + fileNames.get(2) + ";!" + fileNames.get(3))).first();

                loadResult = searchIndex();
                System.out.println("Load result after load 3,4 files: = " + loadResult + " , at study : " + studyId);
                checkLoadResult(expected, loadResult);
                checkVariantSearchIndex(dbAdaptor);


                //////////////////////
                QueryOptions statsOptions = new QueryOptions(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                        .append(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100)
                        .append(DefaultVariantStatisticsManager.OUTPUT, outputUri)
                        .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, "stats");
                storageEngine.calculateStats(studyMetadata.getName(), Collections.singletonList("ALL"), statsOptions);

                query = new Query(VariantQueryParam.STUDY.key(), studyId);
                expected = dbAdaptor.count(query).first();
                loadResult = searchIndex();
                System.out.println("Load result after stats calculate: = " + loadResult + " , at study : " + studyId);
                checkLoadResult(expected, loadResult);
                checkVariantSearchIndex(dbAdaptor);

                //////////////////////
                expected = dbAdaptor.count((Query) null).first();
                loadResult = searchIndex(true);
                System.out.println("Load result overwrite: = " + loadResult + " , at study : " + studyId);
                checkLoadResult(expected, loadResult);
                checkVariantSearchIndex(dbAdaptor);

                //////////////////////
                loadResult = searchIndex();
                System.out.println("Load result nothing to do: = " + loadResult + " , at study : " + studyId);
                checkLoadResult(0, loadResult);
                checkVariantSearchIndex(dbAdaptor);

                /////////// NEW STUDY ///////////
                studyCounter++;
                studyMetadata = metadataManager.createStudy("S_" + studyCounter);
                studyId = studyMetadata.getId();
                inputFiles.clear();
                fileNames.clear();
                if (studyId > maxStudies) {
                    break;
                }
            }
        }

        checkVariantSearchIndex(dbAdaptor);
    }

    public void checkLoadResult(long expected, VariantSearchLoadResult loadResult) {
        assertEquals(expected, loadResult.getNumLoadedVariants());
        if (expected != loadResult.getNumProcessedVariants()) {
            System.err.println("More object than needed were fetched from the DB");
        }
    }

    @Test
    public void testSearchIndexRemoveSearchIndex() throws Exception {
        testRemoveFiles(true);
    }

    @Test
    public void testSearchIndexAfterRemove() throws Exception {
        testRemoveFiles(false);
    }

    public void testRemoveFiles(boolean searchIndexBeforeRemove) throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        VariantStorageEngine storageEngine = getVariantStorageEngine();
        QueryOptions options = new QueryOptions();

        List<URI> inputFiles = new ArrayList<>();
        List<String> fileNames = new ArrayList<>();
        StudyMetadata studyMetadata = metadataManager.createStudy("S_1");
        int studyId = studyMetadata.getId();
        for (int fileId = 12877; fileId <= 12877 + 4; fileId++) {
            String fileName = "1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            URI inputFile = getResourceUri("platinum/" + fileName);
            fileNames.add(fileName);
            inputFiles.add(inputFile);

        }

//        dbAdaptor.getMetadataManager().updateStudyMetadata(studyMetadata, null);
        options.put(VariantStorageOptions.STUDY.key(), studyId);
        storageEngine.getOptions().putAll(options);
        storageEngine.index(inputFiles, outputUri, true, true, true);

        Query query = new Query(VariantQueryParam.STUDY.key(), studyMetadata.getId());
        long expected;
        if (searchIndexBeforeRemove) {
            expected = dbAdaptor.count(query).first();
            VariantSearchLoadResult loadResult = searchIndex();
            System.out.println("Load result after load 1-4 files: = " + loadResult);
            checkLoadResult(expected, loadResult);

            //////////////////////
            storageEngine.removeFiles(studyMetadata.getName(), Collections.singletonList(fileNames.get(0)));
            expected = 0;

        } else {
            //////////////////////
            storageEngine.removeFiles(studyMetadata.getName(), Collections.singletonList(fileNames.get(0)));
            expected = dbAdaptor.count(query).first();
        }

        VariantSearchLoadResult loadResult = searchIndex();
        checkLoadResult(expected, loadResult);
        System.out.println("Load result after remove: = " + loadResult);


        checkVariantSearchIndex(dbAdaptor);
    }

    public void checkVariantSearchIndex(VariantDBAdaptor dbAdaptor) throws IOException, VariantSearchException, StorageEngineException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.LIMIT, 1000);
        Query query = new Query();

        TreeSet<Variant> variantsFromSearch = new TreeSet<>(Comparator.comparing(Variant::toString));
        TreeSet<Variant> variantsFromDB = new TreeSet<>(Comparator.comparing(Variant::toString));

        variantsFromSearch.addAll(variantStorageEngine.getVariantSearchManager().query(DB_NAME, query, queryOptions).getResults());
        variantsFromDB.addAll(dbAdaptor.get(query, queryOptions).getResults());

        assertEquals(variantsFromDB.size(), variantsFromSearch.size());
        assertEquals(variantsFromDB.size(), variantStorageEngine.getVariantSearchManager().count(DB_NAME, query));

        Iterator<Variant> variantsFromSearchIterator = variantsFromSearch.iterator();
        Iterator<Variant> variantsFromDBIterator = variantsFromDB.iterator();
        for (int i = 0; i < variantsFromDB.size(); i++) {
            Variant variantFromDB = variantsFromSearchIterator.next();
            Set<String> studiesFromDB = variantFromDB.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());
            Variant variantFromSearch = variantsFromDBIterator.next();
            Set<String> studiesFromSearch = variantFromSearch.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());

            assertEquals(variantFromDB.toString(), variantFromSearch.toString());
            assertEquals(variantFromDB.toString(), studiesFromDB, studiesFromSearch);
        }
    }

    public final VariantSearchLoadResult searchIndex() throws Exception {
        return searchIndex(false);
    }

    public VariantSearchLoadResult searchIndex(boolean overwrite) throws Exception {
        return variantStorageEngine.secondaryIndex(new Query(), new QueryOptions(), overwrite);
    }
}
