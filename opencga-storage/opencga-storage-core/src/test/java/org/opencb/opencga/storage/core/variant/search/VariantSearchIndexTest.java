package org.opencb.opencga.storage.core.variant.search;

import org.junit.*;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantSearchIndexTest extends VariantStorageBaseTest {

    @Rule
    public VariantSolrExternalResource solr = new VariantSolrExternalResource(this);

    @Before
    @Override
    public void before() throws Exception {
        super.before();
        solr.configure(variantStorageEngine);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testIncrementalIndex() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        QueryOptions options = new QueryOptions();

        int maxStudies = 2;
        int studyCounter = 0;
        int release = 1;
        List<URI> inputFiles = new ArrayList<>();
        List<String> fileNames = new ArrayList<>();
        for (List<Integer> batch : BatchUtils.splitBatches(IntStream.range(12877, 12894).boxed().collect(Collectors.toList()), 4)) {

            /////////// NEW STUDY ///////////
            studyCounter++;
            if (studyCounter > maxStudies) {
                break;
            }
            String studyName = "S_" + studyCounter;

            List<URI> batchInputFiles = new ArrayList<>();
            List<String> batchFileNames = new ArrayList<>();
            for (Integer fileId : batch) {
                URI inputFile = getPlatinumFile(fileId);
                batchFileNames.add(Paths.get(inputFile).getFileName().toString());
                batchInputFiles.add(inputFile);
            }
            inputFiles.addAll(batchInputFiles);
            fileNames.addAll(batchFileNames);

            options.put(VariantStorageOptions.STUDY.key(), studyName);
            variantStorageEngine.getOptions().putAll(options);
            variantStorageEngine.getOptions().put(VariantStorageOptions.RELEASE.key(), release++);
            variantStorageEngine.index(inputFiles.subList(0, 2), outputUri, true, true, true);

            StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyName);
            int studyId = studyMetadata.getId();

            Query query = new Query(VariantQueryParam.STUDY.key(), studyName);
            long expected = dbAdaptor.count(query).first();

            VariantSearchLoadResult loadResult = searchIndex();
            System.out.println("Load result after load 1,2 files: = " + loadResult + ", at study : " + studyId);
            checkLoadResult(expected, expected, 0, loadResult);
            checkVariantSearchIndex(dbAdaptor);

            //////////////////////
            variantStorageEngine.getOptions().putAll(options);
            variantStorageEngine.getOptions().put(VariantStorageOptions.RELEASE.key(), release++);
            variantStorageEngine.index(inputFiles.subList(2, 4), outputUri, true, true, true);

            expected = dbAdaptor.count(query.append(VariantQueryParam.FILE.key(),
                    "!" + fileNames.get(0) + ";!" + fileNames.get(1) + ";" + fileNames.get(2) + ";" + fileNames.get(3))).first();
            expected += dbAdaptor.count(query.append(VariantQueryParam.FILE.key(),
                    "!" + fileNames.get(0) + ";!" + fileNames.get(1) + ";!" + fileNames.get(2) + ";" + fileNames.get(3))).first();
            expected += dbAdaptor.count(query.append(VariantQueryParam.FILE.key(),
                    "!" + fileNames.get(0) + ";!" + fileNames.get(1) + ";" + fileNames.get(2) + ";!" + fileNames.get(3))).first();

            loadResult = searchIndex();
            System.out.println("Load result after load 3,4 files: = " + loadResult + " , at study : " + studyId);
            checkLoadResult(expected, expected, 0, loadResult);
            checkVariantSearchIndex(dbAdaptor);

            //////////////////////
            // Only "new variants" expected to be annotated
            expected = variantStorageEngine.annotate(outputUri, new ObjectMap());
            loadResult = searchIndex(false);
            System.out.println("Load result after annotate: = " + loadResult + " , at study : " + studyId);
            checkLoadResult(expected, expected, 0, loadResult);
            checkVariantSearchIndex(dbAdaptor);

            //////////////////////
            QueryOptions statsOptions = new QueryOptions(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                    .append(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100)
                    .append(DefaultVariantStatisticsManager.OUTPUT, outputUri)
                    .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, "stats");
            variantStorageEngine.calculateStats(studyMetadata.getName(), Collections.singletonList("ALL"), statsOptions);

            query = new Query(VariantQueryParam.STUDY.key(), studyId);
            expected = dbAdaptor.count(query).first();
            loadResult = searchIndex();
            System.out.println("Load result after stats calculate: = " + loadResult + " , at study : " + studyId);
            // Only stats updates are expected
            checkLoadResult(expected, 0, expected, loadResult);
            //     checkLoadResult(expected, loadResult);
            //     // Not all variants are expected to be updated. Those without new relevant GTs will not be updated
            //     assertThat(loadResult.getNumLoadedVariantsPartialStatsUpdate(), VariantMatchers.lt(expected));
            //     assertThat(loadResult.getNumLoadedVariantsPartialStatsUpdate(), VariantMatchers.gt(0L));
            checkVariantSearchIndex(dbAdaptor);

            //////////////////////
            expected = dbAdaptor.count((Query) null).first();
            loadResult = searchIndex(true);
            System.out.println("Load result overwrite: = " + loadResult + " , at study : " + studyId);
            checkLoadResult(expected, expected, 0, loadResult);
            checkVariantSearchIndex(dbAdaptor);

            //////////////////////
            loadResult = searchIndex();
            System.out.println("Load result nothing to do: = " + loadResult + " , at study : " + studyId);
            checkLoadResult(0, 0, 0, loadResult);
            checkVariantSearchIndex(dbAdaptor);
        }

        checkVariantSearchIndex(dbAdaptor);
    }

    @Test
    public void testIncrementalStatsIndex() throws Exception {
        // Plan:
        // - Load all 17 platinum files
        // - Annotate
        // - Create a cohort with 1 sample
        // - Secondary annotation index - all variants should be updated.
        // - Calculate stats and run secondary index
        // - For each remaining sample
        //   - Add 1 sample to the cohort
        //   - Calculate stats
        //   - Secondary index should add as many documents as variants had that added sample
        String study = "Study";
        StudyMetadata studyMetadata = metadataManager.createStudy(study);
        for (int i = 0; i < 17; i++) {
            URI platinumFile = getPlatinumFile(i);

            QueryOptions options = new QueryOptions().append(VariantStorageOptions.STUDY.key(), study);
            variantStorageEngine.getOptions().putAll(options);
            variantStorageEngine.index(Collections.singletonList(platinumFile), outputUri, true, true, true);
        }

        variantStorageEngine.annotate(outputUri, new ObjectMap());

        LinkedList<String> allSamples = new LinkedList<>();
        for (SampleMetadata sampleMetadata : metadataManager.sampleMetadataIterable(studyMetadata.getId())) {
            allSamples.add(sampleMetadata.getName());
        }
        Map<String, List<String>> cohortsMap = new HashMap<>();
        ArrayList<String> samples = new ArrayList<>();
        samples.add(allSamples.removeFirst());
        cohortsMap.put("cohort", samples);
        variantStorageEngine.calculateStats(study, cohortsMap, new QueryOptions());
        long numVariants = variantStorageEngine.count(new Query()).first();

        VariantSearchLoadResult loadResult = searchIndex();
        checkLoadResult(numVariants, numVariants, 0, loadResult);

        for (String sample : allSamples) {
            samples.add(sample);
            cohortsMap.put("cohort", samples);
            variantStorageEngine.calculateStats(study, cohortsMap, new QueryOptions());
            Integer fileId = variantStorageEngine.getMetadataManager().getSampleMetadata(studyMetadata.getId(), sample).getFiles().get(0);
            VariantSetStats stats = variantStorageEngine.getMetadataManager().getVariantFileMetadata(studyMetadata.getId(), fileId, null).first().getStats();
            long expected = stats.getTypeCount().entrySet().stream()
                    .filter(e-> !e.getKey().equals(VariantType.NO_VARIATION.name()))
                    .map(Map.Entry::getValue)
                    .mapToLong(Long::longValue).sum();

            loadResult = searchIndex();
            System.out.println("Load result after adding sample " + (samples.size() - 1) + " " + sample + ": = " + loadResult);
            checkLoadResult(expected, 0, expected, loadResult);
        }

    }

    public void checkLoadResult(long expected, long expectedInserts, long expectedPartial, VariantSearchLoadResult loadResult) {
        assertEquals(expectedInserts, loadResult.getNumInsertedVariants());
        assertEquals(expectedPartial, loadResult.getNumLoadedVariantsPartialStatsUpdate());
        checkLoadResult(expected, loadResult);
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
        variantStorageEngine.getOptions().putAll(options);
        variantStorageEngine.index(inputFiles, outputUri, true, true, true);

        Query query = new Query(VariantQueryParam.STUDY.key(), studyMetadata.getId());
        long expected;
        if (searchIndexBeforeRemove) {
            expected = dbAdaptor.count(query).first();
            VariantSearchLoadResult loadResult = searchIndex();
            System.out.println("Load result after load 1-4 files: = " + loadResult);
            checkLoadResult(expected, expected, 0, loadResult);

            //////////////////////
            variantStorageEngine.removeFiles(studyMetadata.getName(), Collections.singletonList(fileNames.get(0)), outputUri);
            try {
                variantStorageEngine.calculateStats(studyMetadata.getName(), Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());
                variantStorageEngine.variantsPrune(false, false, outputUri);
            } catch (UnsupportedOperationException ignored) {}
            expected = 0;

        } else {
            //////////////////////
            variantStorageEngine.removeFiles(studyMetadata.getName(), Collections.singletonList(fileNames.get(0)), outputUri);
            expected = dbAdaptor.count(query).first();
        }

        VariantSearchLoadResult loadResult = searchIndex();
        checkLoadResult(expected, expected, 0, loadResult);
        System.out.println("Load result after remove: = " + loadResult);


        checkVariantSearchIndex(dbAdaptor);
    }

    public void checkVariantSearchIndex(VariantDBAdaptor dbAdaptor) throws Exception {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.LIMIT, 1000);
        Query query = new Query();

        SearchIndexMetadata indexMetadata = metadataManager.getProjectMetadata().getSecondaryAnnotationIndex()
                .getLastStagingOrActiveIndex();
        assertTrue(variantStorageEngine.getVariantSearchManager().tlogReplicasInSync(indexMetadata));

        TreeSet<Variant> variantsFromSearch = new TreeSet<>(Comparator.comparing(Variant::toString));
        TreeSet<Variant> variantsFromDB = new TreeSet<>(Comparator.comparing(Variant::toString));

        variantsFromSearch.addAll(variantStorageEngine.getVariantSearchManager().query(indexMetadata, variantStorageEngine.parseQuery(query, queryOptions)).getResults());
        variantsFromDB.addAll(dbAdaptor.get(query, queryOptions).getResults());

        assertEquals(variantsFromDB.size(), variantsFromSearch.size());
        assertEquals(variantsFromDB.size(), variantStorageEngine.getVariantSearchManager().count(indexMetadata, query));

        Iterator<Variant> variantsFromSearchIterator = variantsFromSearch.iterator();
        Iterator<Variant> variantsFromDBIterator = variantsFromDB.iterator();
        for (int i = 0; i < variantsFromDB.size(); i++) {
            Variant variantFromDB = variantsFromDBIterator.next();
            Set<String> studiesFromDB = variantFromDB.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());
            Variant variantFromSearch = variantsFromSearchIterator.next();
            Set<String> studiesFromSearch = variantFromSearch.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());

            assertEquals(variantFromDB.toString(), variantFromSearch.toString());
            assertEquals(variantFromDB.toString(), studiesFromDB, studiesFromSearch);
        }
    }

    public final VariantSearchLoadResult searchIndex() throws Exception {
        return searchIndex(false);
    }

    public VariantSearchLoadResult searchIndex(boolean overwrite) throws Exception {
        solr.configure(variantStorageEngine);
        VariantSearchLoadResult result = variantStorageEngine.secondaryIndex(new Query(), new QueryOptions(), overwrite);
        solr.printCollections(Paths.get(newOutputUri("searchIndex_" + TimeUtils.getTime() + "_solr")));
        return result;
    }
}
