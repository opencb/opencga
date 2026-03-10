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

package org.opencb.opencga.storage.mongodb.variant;

import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.opencb.biodata.formats.variant.vcf4.FullVcfCodec;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageConverterTask;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageReader;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBOperations;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBVariantMergeLoader;
import org.opencb.opencga.storage.mongodb.variant.load.variants.MongoDBVariantMerger;

import java.util.*;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_FIELD;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class VariantMongoDBWriterTest implements MongoDBVariantStorageTest {

    private Query query;
    private static final QueryOptions QUERY_OPTIONS = new QueryOptions(QueryOptions.SORT, true);
    private static MongoDBVariantStorageEngine variantStorageManager;
    private StudyMetadata studyMetadata1, studyMetadata2;
    // File IDs as registered in the metadata manager
    private int fileId1, fileId2, fileId3;
    private int studyId1, studyId2;
    private String studyName1 = "Study 1";
    private String studyName2 = "Study 2";
    private VariantMongoDBAdaptor dbAdaptor;
    // Sample names per file
    private List<String> file1SampleNames = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");
    private List<String> file2SampleNames = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");
    private List<String> file3SampleNames = Arrays.asList("NA00001.X", "NA00002.X", "NA00003.X", "NA00004.X");

    @Parameters
    public static List<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean cleanWhileLoading : new boolean[]{true, false}) {
            for (String defaultGenotype : Arrays.asList("0/0")) {
                for (boolean ignoreOverlapping : new boolean[]{true, false}) {
                    parameters.add(new Object[]{cleanWhileLoading, defaultGenotype, ignoreOverlapping});
                }
            }
        }
        return parameters;
    }

    @Parameter
    public boolean cleanWhileLoading;

    @Parameter(1)
    public String defaultGenotype;

    @Parameter(2)
    public boolean ignoreOverlappingVariants;

    @Before
    public void setUp() throws Exception {
        System.out.println("====================================================");
        System.out.println(" #        CONFIGURATION");
        System.out.println(" # cleanWhileLoading = " + cleanWhileLoading);
        System.out.println(" # defaultGenotype = " + defaultGenotype);
        System.out.println(" # overlappingVariants = " + !ignoreOverlappingVariants);
        System.out.println(" #     (ignoreOverlapping = " + ignoreOverlappingVariants + ')');
        System.out.println("====================================================");
        Configurator.setRootLevel(Level.DEBUG);

        // Close engine to reset cached dbAdaptor/metadataManager before clearing DB
        if (variantStorageManager != null) {
            variantStorageManager.close();
        }
        clearDB(VariantStorageBaseTest.DB_NAME);
        variantStorageManager = getVariantStorageEngine();
        dbAdaptor = variantStorageManager.getDBAdaptor();
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();

        // Create studies
        studyMetadata1 = mm.createStudy(studyName1);
        studyId1 = studyMetadata1.getId();
        studyMetadata1.getAttributes().append(DEFAULT_GENOTYPE.key(), defaultGenotype);
        mm.unsecureUpdateStudyMetadata(studyMetadata1);

        studyMetadata2 = mm.createStudy(studyName2);
        studyId2 = studyMetadata2.getId();
        studyMetadata2.getAttributes().append(DEFAULT_GENOTYPE.key(), defaultGenotype);
        mm.unsecureUpdateStudyMetadata(studyMetadata2);

        // Register files + samples via metadata manager (IDs are auto-assigned)
        fileId1 = mm.registerFile(studyId1, "file1.vcf", file1SampleNames);
        fileId2 = mm.registerFile(studyId2, "file2.vcf", file2SampleNames);
        fileId3 = mm.registerFile(studyId2, "file3.vcf", file3SampleNames);

        // Reload study metadata after file registration
        studyMetadata1 = mm.getStudyMetadata(studyId1);
        studyMetadata2 = mm.getStudyMetadata(studyId2);

        query = new Query()
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL);
        if (defaultGenotype.equals("0/0")) {
            query.append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "?/?");
        }
    }


    @After
    public void shutdown() throws Exception {
    }

    private void setExtraFormatFields() {
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        studyMetadata1.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyMetadata1.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        mm.unsecureUpdateStudyMetadata(studyMetadata1);
        studyMetadata2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyMetadata2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));
        mm.unsecureUpdateStudyMetadata(studyMetadata2);
    }

    /**
     * Insert some variants.
     *             +-------+---------------+
     *             | Study1|    Study2     |
     * +-----------|-------+---------------+
     * | Variant   | File1 | File2 | File3 |
     * +-----------+-------+-------+-------+  // Check merging having other loaded studies
     * | 999       |   x   |       |       |
     * | 1000      |   x   |   x   |   x   |
     * | 1002      |   x   |       |   x   |
     * | 1004      |       |   x   |       |
     * | 1006      |       |       |   x   |
     * +-----------+-------+-------+-------+
     *
     */
    @Test
    public void testInsertMultiFiles() throws StorageEngineException {
        List<Variant> allVariants;
        setExtraFormatFields();

        assertEqualsResult(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), loadFile1());
        allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
        assertEquals(3, allVariants.size());

        assertEqualsResult(new MongoDBVariantWriteResult(1, 1, 0, 0, 0, 0), loadFile2());
        allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
        assertEquals(4, allVariants.size());

        assertEqualsResult(new MongoDBVariantWriteResult(1, 2, 1, 0, 0, 0), loadFile3());
        allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
        assertEquals(5, allVariants.size());

        checkLoadedVariants(allVariants);

    }

    @Test
    public void testInsertMultiFilesMultiMerge() throws StorageEngineException {
        List<Variant> allVariants;
        setExtraFormatFields();

        assertEqualsResult(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), loadFile1());
        allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
        assertEquals(3, allVariants.size());

        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();
        writeResult.merge(stageVariants(studyId2, createFile2Variants(), fileId2));
        writeResult.merge(stageVariants(studyId2, createFile3Variants(), fileId3));
        writeResult = mergeVariants(studyMetadata2, Arrays.asList(fileId2, fileId3), writeResult, Collections.emptyList());
        assertEqualsResult(new MongoDBVariantWriteResult(2, 2, 0, 0, 0, 0), writeResult);
        allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
        assertEquals(5, allVariants.size());

        checkLoadedVariants(allVariants);

    }

    /**
     * Insert variants chromosome by chromosome
     *
     * @throws StorageEngineException if error
     */
    @Test
    public void testInsertMultiFilesMultipleRegions() throws StorageEngineException {
        List<Variant> allVariants;
        setExtraFormatFields();

        for (String chr : Arrays.asList("1", "2", "3")) {
            Query query = getQuery(chr);

            int f1 = registerExtraFile(studyId1, file1SampleNames);
            MongoDBVariantWriteResult writeResult2 = loadFile(studyMetadata1,
                    createFile1Variants(chr, String.valueOf(f1), String.valueOf(studyId1)), f1, Collections.singletonList(chr));
            assertEqualsResult(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), writeResult2);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(3, allVariants.size());

            int f2 = registerExtraFile(studyId2, file2SampleNames);
            MongoDBVariantWriteResult writeResult1 = loadFile(studyMetadata2,
                    createFile2Variants(chr, String.valueOf(f2), String.valueOf(studyId2)), f2, Collections.singletonList(chr));
            assertEqualsResult(new MongoDBVariantWriteResult(1, 1, 0, 0, 0, 0), writeResult1);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(4, allVariants.size());

            int f3 = registerExtraFile(studyId2, file3SampleNames);
            MongoDBVariantWriteResult writeResult = loadFile(studyMetadata2,
                    createFile3Variants(chr, String.valueOf(f3), String.valueOf(studyId2)), f3, Collections.singletonList(chr));
            assertEqualsResult(new MongoDBVariantWriteResult(1, 2, 1, 0, 0, 0), writeResult);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(5, allVariants.size());

            checkLoadedVariants(allVariants, new int[]{f1, f2, f3});
        }
    }

    /**
     * Insert variants study by study
     *
     * @throws StorageEngineException if error
     */
    @Test
    public void testInsertMultiFilesMultipleRegionsStudyByStudy() throws StorageEngineException {
        List<Variant> allVariants;
        setExtraFormatFields();

        List<String> chromosomes = Arrays.asList("1", "2", "3", "4");
        Map<String, int[]> mapFileIds = new HashMap<>();
        for (String chr : chromosomes) {
            int f2 = registerExtraFile(studyId2, file2SampleNames);
            int f3 = registerExtraFile(studyId2, file3SampleNames);
            int f1 = registerExtraFile(studyId1, file1SampleNames);
            mapFileIds.put(chr, new int[]{f1, f2, f3});
            Query query = getQuery(chr);

            assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 0, 0, 0, 0),
                    loadFile(studyMetadata2, createFile2Variants(chr, String.valueOf(f2), String.valueOf(studyId2)),
                            f2, Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(2, allVariants.size());

            assertEqualsResult(new MongoDBVariantWriteResult(2, 1, 1, 0, 0, 0),
                    loadFile(studyMetadata2, createFile3Variants(chr, String.valueOf(f3), String.valueOf(studyId2)),
                            f3, Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(4, allVariants.size());
        }

        for (String chr : chromosomes) {
            Query query = getQuery(chr);
            int f1 = mapFileIds.get(chr)[0];

            assertEqualsResult(new MongoDBVariantWriteResult(1, 2, 0, 0, 0, 0),
                    loadFile(studyMetadata1, createFile1Variants(chr, String.valueOf(f1), String.valueOf(studyId1)),
                            f1, Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(5, allVariants.size());
        }

        for (String chr : chromosomes) {
            Query query = getQuery(chr);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            checkLoadedVariants(allVariants, mapFileIds.get(chr));
        }
    }


    /**
     * Insert variants study by study
     *
     * @throws StorageEngineException if error
     */
    @Test
    public void testInsertMultiFilesMultipleRegionsStudyByStudy2() throws StorageEngineException {
        List<Variant> allVariants;
        setExtraFormatFields();

        List<String> chromosomes = Arrays.asList("1", "2", "X", "3", "5", "4");
        Map<String, int[]> mapFileIds = new HashMap<>();
        for (String chr : chromosomes) {
            int f1 = registerExtraFile(studyId1, file1SampleNames);
            int f2 = registerExtraFile(studyId2, file2SampleNames);
            int f3 = registerExtraFile(studyId2, file3SampleNames);
            mapFileIds.put(chr, new int[]{f1, f2, f3});
            Query query = getQuery(chr);

            assertEqualsResult(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0),
                    loadFile(studyMetadata1, createFile1Variants(chr, String.valueOf(f1), String.valueOf(studyId1)),
                            f1, Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(3, allVariants.size());
        }

        for (String chr : chromosomes) {
            Query query = getQuery(chr);
            int f2 = mapFileIds.get(chr)[1];
            int f3 = mapFileIds.get(chr)[2];

            assertEqualsResult(new MongoDBVariantWriteResult(1, 1, 0, 0, 0, 0),
                    loadFile(studyMetadata2, createFile2Variants(chr, String.valueOf(f2), String.valueOf(studyId2)),
                            f2, Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(4, allVariants.size());

            assertEqualsResult(new MongoDBVariantWriteResult(1, 2, 1, 0, 0, 0),
                    loadFile(studyMetadata2, createFile3Variants(chr, String.valueOf(f3), String.valueOf(studyId2)),
                            f3, Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(5, allVariants.size());
        }

        for (String chr : chromosomes) {
            Query query = getQuery(chr);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            checkLoadedVariants(allVariants, mapFileIds.get(chr));
        }
    }

    /**
     * Register an additional file for multi-region tests.
     * Uses a unique auto-incrementing name so that each chromosome gets its own file in the metadata manager.
     */
    private int registerExtraFile(int studyId, List<String> sampleNames) throws StorageEngineException {
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        String fileName = "extra_" + studyId + "_" + UUID.randomUUID() + ".vcf";
        return mm.registerFile(studyId, fileName, sampleNames);
    }

    public Query getQuery(String chr) {
        return new Query(this.query).append(VariantQueryParam.REGION.key(), chr);
    }

    public void checkLoadedVariants(List<Variant> allVariants) {
        checkLoadedVariants(allVariants, new int[]{fileId1, fileId2, fileId3});
    }

    public void checkLoadedVariants(List<Variant> allVariants, int[] fileIds) {
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        Variant variant;
        variant = allVariants.get(0);
        assertEquals(999, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName1), variant.getStudiesMap().keySet());

        // file1 DP=[11,12,13,14], file2 DP=[1,2,3,4], file3 DP=[5,6,7,8]
        variant = allVariants.get(1);
        assertEquals(1000, variant.getStart().longValue());
        assertEquals(new HashSet<>(Arrays.asList(studyName1, studyName2)), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyName1, studyId1, fileIds[0], mm,
                (pos) -> Integer.toString(pos + 11), "DP");

        variant = allVariants.get(2);
        assertEquals(1002, variant.getStart().longValue());
        assertEquals(new HashSet<>(Arrays.asList(studyName1, studyName2)), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyName1, studyId1, fileIds[0], mm,
                (pos) -> Integer.toString(pos + 11), "DP");
        // File2 not present at 1002 → samples get null for extra fields
        checkSampleData(variant, studyName2, studyId2, fileIds[1], mm,
                (pos) -> null, "DP");
        checkSampleData(variant, studyName2, studyId2, fileIds[1], mm,
                (pos) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyName2, studyId2, fileIds[2], mm,
                (pos) -> Integer.toString(pos + 5), "DP");

        variant = allVariants.get(3);
        assertEquals(1004, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName2), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyName2, studyId2, fileIds[1], mm,
                (pos) -> Integer.toString(pos + 1), "DP");
        // File3 not present at 1004 → samples get null for extra fields
        checkSampleData(variant, studyName2, studyId2, fileIds[2], mm,
                (pos) -> null, "DP");
        checkSampleData(variant, studyName2, studyId2, fileIds[2], mm,
                (pos) -> UNKNOWN_GENOTYPE, "GT");
        // file2 variant 1004 GQX: positions 0,2 have "0.7"; positions 1,3 have "." and ".."
        checkSampleData(variant, studyName2, studyId2, fileIds[1], mm,
                (pos) -> pos % 2 == 0 ? "0.7" : UNKNOWN_FIELD, "GQX");
        checkSampleData(variant, studyName2, studyId2, fileIds[2], mm,
                (pos) -> null, "GQX");

        variant = allVariants.get(4);
        assertEquals(1006, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName2), variant.getStudiesMap().keySet());
        // File2 not present at 1006 → samples get null for extra fields
        checkSampleData(variant, studyName2, studyId2, fileIds[1], mm,
                (pos) -> null, "DP");
        checkSampleData(variant, studyName2, studyId2, fileIds[1], mm,
                (pos) -> null, "GQX");
        checkSampleData(variant, studyName2, studyId2, fileIds[1], mm,
                (pos) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyName2, studyId2, fileIds[2], mm,
                (pos) -> Integer.toString(pos + 5), "DP");
        checkSampleData(variant, studyName2, studyId2, fileIds[2], mm,
                (pos) -> "0.7", "GQX");
    }


    /**
     * @param valueProvider receives 0-based sample position within the file
     */
    public void checkSampleData(Variant variant, String studyName, int studyId, int fileId,
                                 VariantStorageMetadataManager mm,
                                 Function<Integer, String> valueProvider, String field) {
        LinkedHashSet<Integer> sampleIds = mm.getFileMetadata(studyId, fileId).getSamples();
        StudyEntry study = variant.getStudy(studyName);
        int samplePos = 0;
        for (Integer sampleId : sampleIds) {
            String sampleName = mm.getSampleName(studyId, sampleId);
            assertTrue("Sample " + sampleName + " not found in study " + studyName,
                    study.getSamplesName().contains(sampleName));
            assertEquals("Variant=" + variant + " Study=" + studyName + " FileId=" + fileId
                            + " Field=" + field + " Sample=" + sampleName + " (pos=" + samplePos + ")\n" + variant.toJson(),
                    valueProvider.apply(samplePos), study.getSampleData(sampleName, field));
            samplePos++;
        }
    }

    public MongoDBVariantWriteResult loadFile1() throws StorageEngineException {
        return loadFile(studyMetadata1,
                createFile1Variants("X", String.valueOf(fileId1), String.valueOf(studyId1)),
                fileId1, Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile2() throws StorageEngineException {
        return loadFile(studyMetadata2,
                createFile2Variants("X", String.valueOf(fileId2), String.valueOf(studyId2)),
                fileId2, Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile3() throws StorageEngineException {
        return loadFile(studyMetadata2,
                createFile3Variants("X", String.valueOf(fileId3), String.valueOf(studyId2)),
                fileId3, Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile(StudyMetadata studyMetadata, List<Variant> variants,
                                               int fileId, List<String> chromosomes) throws StorageEngineException {
        MongoDBVariantWriteResult stageWriteResult = stageVariants(studyMetadata.getId(), variants, fileId);
        return mergeVariants(studyMetadata, Collections.singletonList(fileId), stageWriteResult, chromosomes);
    }

    public MongoDBVariantWriteResult stageVariants(int studyId, List<Variant> variants, int fileId) {
        MongoDBCollection stage = dbAdaptor.getStageCollection(studyId);
        MongoDBVariantStageLoader variantStageLoader = new MongoDBVariantStageLoader(stage, studyId, fileId, false);
        MongoDBVariantStageConverterTask converterTask = new MongoDBVariantStageConverterTask();

        variantStageLoader.write(converterTask.apply(variants));

        return variantStageLoader.getWriteResult();
    }

    public MongoDBVariantWriteResult mergeVariants(StudyMetadata studyMetadata, int fileId,
                                                   MongoDBVariantWriteResult stageWriteResult) throws StorageEngineException {

        return mergeVariants(studyMetadata, Collections.singletonList(fileId), stageWriteResult, Collections.emptyList());
    }

    public MongoDBVariantWriteResult mergeVariants(StudyMetadata studyMetadata, List<Integer> fileIds,
                                                   MongoDBVariantWriteResult stageWriteResult,
                                                   List<String> chromosomes) throws StorageEngineException {
        int studyId = studyMetadata.getId();
        MongoDBCollection stage = dbAdaptor.getStageCollection(studyId);
        MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();
        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(stage, studyId, chromosomes);
        MongoDBVariantMerger dbMerger = new MongoDBVariantMerger(
                dbAdaptor, studyMetadata, fileIds, false, ignoreOverlappingVariants, 1);
        boolean resume = false;
        MongoDBVariantMergeLoader variantLoader = new MongoDBVariantMergeLoader(
                variantsCollection, dbAdaptor.getStageCollection(studyId),
                dbAdaptor.getStudiesCollection(), studyMetadata, fileIds, resume, cleanWhileLoading, null);

        reader.open();
        reader.pre();

        List<Document> batch = reader.read(100);
        while (batch != null && !batch.isEmpty()) {
            List<MongoDBOperations> apply = dbMerger.apply(batch);
            variantLoader.write(apply);
            batch = reader.read(100);
        }

        reader.post();
        reader.close();

        long cleanedDocuments = MongoDBVariantStageLoader.cleanStageCollection(stage, studyId, fileIds, null, null);
        if (cleanWhileLoading) {
            assertEquals(0, cleanedDocuments);
        } else {
            assertNotEquals(0, cleanedDocuments);
        }

        // Mark files as indexed
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        mm.addIndexedFiles(studyId, fileIds);

        return variantLoader.getResult().setSkippedVariants(stageWriteResult != null ? stageWriteResult.getSkippedVariants() : 0);
    }

    public List<Variant> createFile1Variants() {
        return createFile1Variants("X", String.valueOf(fileId1), String.valueOf(studyId1));
    }
    public List<Variant> createFile2Variants() {
        return createFile2Variants("X", String.valueOf(fileId2), String.valueOf(studyId2));
    }
    public List<Variant> createFile3Variants() {
        return createFile3Variants("X", String.valueOf(fileId3), String.valueOf(studyId2));
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile1Variants(String chromosome, String fileId, String studyId) {

        List<Variant> variants = new LinkedList<>();
        variants.add(Variant.newBuilder(chromosome, 999, 999, "A", "C")
                .setStudyId(studyId)
                .setFileId(fileId)
                .setSampleDataKeys("GT", "DP", "GQX")
                .addSample("NA19600", "./.", "11", "0.7")
                .addSample("NA19660", "1/1", "12", "0.7")
                .addSample("NA19661", "0/0", "13", "0.7")
                .addSample("NA19685", "1/0", "14", "0.7")
                .build());

        variants.add(Variant.newBuilder(chromosome, 1000, 1000, "A", "C")
                .setStudyId(studyId)
                .setFileId(fileId)
                .setSampleDataKeys("GT", "DP", "GQX")
                .addSample("NA19600", "./.", "11", "0.7")
                .addSample("NA19660", "1/1", "12", "0.7")
                .addSample("NA19661", "0/0", "13", "0.7")
                .addSample("NA19685", "1/0", "14", "0.7")
                .build());

        variants.add(Variant.newBuilder(chromosome, 1002, 1002, "A", "C")
                .setStudyId(studyId)
                .setFileId(fileId)
                .setSampleDataKeys("GT", "DP", "GQX")
                .addSample("NA19600", "0/1", "11", "0.7")
                .addSample("NA19660", "0/0", "12", "0.7")
                .addSample("NA19661", "1/0", "13", "0.7")
                .addSample("NA19685", "0/0", "14", "0.7")
                .build());

        return variants;
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile2Variants(String chromosome, String fileId, String studyId) {
        List<Variant> variants = new LinkedList<>();

        variants.add(Variant.newBuilder(chromosome, 1000, 1000, "A", "C")
                .setStudyId(studyId)
                .setFileId(fileId)
                .setSampleDataKeys("GT", "DP", "GQX")
                .addSample("NA19600", "./.", "1", "0.7")
                .addSample("NA19660", "1/1", "2", "0.7")
                .addSample("NA19661", "0/0", "3", "0.7")
                .addSample("NA19685", "1/0", "4", "0.7")
                .build());

        variants.add(Variant.newBuilder(chromosome, 1004, 1004, "A", "C")
                .setStudyId(studyId)
                .setFileId(fileId)
                .setSampleDataKeys("GT", "DP", "GQX")
                .addSample("NA19600", "0/1", "1", "0.7")
                .addSample("NA19660", "0/0", "2", ".")
                .addSample("NA19661", "1/0", "3", "0.7")
                .addSample("NA19685", "0/0", "4", "..")
                .build());

        return variants;
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile3Variants(String chromosome, String fileId, String studyId) {

        List<Variant> variants = new LinkedList<>();

        variants.add(Variant.newBuilder(chromosome, 1000, 1000, "A", "C")
                .setStudyId(studyId)
                .setFileId(fileId)
                .setSampleDataKeys("GT", "DP", "GQX")
                .addSample("NA00001.X", "0/1", "5", "0.7")
                .addSample("NA00002.X", "0/0", "6", "0.7")
                .addSample("NA00003.X", "1/0", "7", "0.7")
                .addSample("NA00004.X", "0/0", "8", "0.7")
                .build());

        variants.add(Variant.newBuilder(chromosome, 1002, 1002, "A", "C")
                .setStudyId(studyId)
                .setFileId(fileId)
                .setSampleDataKeys("GT", "DP", "GQX")
                .addSample("NA00001.X", "0/1", "5", "0.7")
                .addSample("NA00002.X", "0/0", "6", "0.7")
                .addSample("NA00003.X", "1/0", "7", "0.7")
                .addSample("NA00004.X", "0/0", "8", "0.7")
                .build());

        variants.add(Variant.newBuilder(chromosome, 1006, 1006, "A", "C")
                .setStudyId(studyId)
                .setFileId(fileId)
                .setSampleDataKeys("GT", "DP", "GQX")
                .addSample("NA00001.X", "0/1", "5", "0.7")
                .addSample("NA00002.X", "0/0", "6", "0.7")
                .addSample("NA00003.X", "1/0", "7", "0.7")
                .addSample("NA00004.X", "0/0", "8", "0.7")
                .build());


        return variants;
    }

    @Test
    public void testInsertSameVariantTwice() throws StorageEngineException {

        loadFile1();
        loadFile2();

        List<Variant> file3Variants = createFile3Variants();
        file3Variants.add(file3Variants.get(2));

        MongoDBVariantWriteResult result = loadFile(studyMetadata2, file3Variants, fileId3, Collections.emptyList());
        assertEqualsResult(new MongoDBVariantWriteResult(0, 2, 1, 0, 0, 2), result);
    }

    @Test
    public void testDuplicatedVariantOnlyOneFile_mergeAtSameTime() throws StorageEngineException {
        List<Variant> file2Variants = createFile2Variants();
        List<Variant> file3Variants = createFile3Variants();
        file3Variants.add(file3Variants.get(0));
        assertThat(file3Variants.get(0), VariantMatchers.overlaps(file2Variants.get(0)));

        stageVariants(studyId2, file3Variants, fileId3);
        stageVariants(studyId2, file2Variants, fileId2);
        MongoDBVariantWriteResult result = mergeVariants(
                studyMetadata2, Arrays.asList(fileId2, fileId3), null, Collections.emptyList());
        assertEqualsResult(new MongoDBVariantWriteResult(4, 0, 0, 0, 0, 2), result);
    }

    @Test
    public void testDuplicatedVariantOnlyOneFile_mergeDuplicatedFirst() throws StorageEngineException {
        List<Variant> file2Variants = createFile2Variants();
        List<Variant> file3Variants = createFile3Variants();
        file3Variants.add(file3Variants.get(0));
        assertThat(file3Variants.get(0), VariantMatchers.overlaps(file2Variants.get(0)));

        stageVariants(studyId2, file3Variants, fileId3);
        stageVariants(studyId2, file2Variants, fileId2);
        MongoDBVariantWriteResult resultMergeFile3 = mergeVariants(studyMetadata2, fileId3, null);
        MongoDBVariantWriteResult resultMergeFile2 = mergeVariants(studyMetadata2, fileId2, null);
        assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 0, 0, 0, 2), resultMergeFile3);
        assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 2, 0, 0, 0), resultMergeFile2);
    }

    @Test
    public void testDuplicatedVariantOnlyOneFile_mergeDuplicatedLast() throws StorageEngineException {
        List<Variant> file2Variants = createFile2Variants();
        List<Variant> file3Variants = createFile3Variants();
        file3Variants.add(file3Variants.get(0));
        assertThat(file3Variants.get(0), VariantMatchers.overlaps(file2Variants.get(0)));

        stageVariants(studyId2, file3Variants, fileId3);
        stageVariants(studyId2, file2Variants, fileId2);
        MongoDBVariantWriteResult resultMergeFile2 = mergeVariants(studyMetadata2, fileId2, null);
        MongoDBVariantWriteResult resultMergeFile3 = mergeVariants(studyMetadata2, fileId3, null);
        assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 0, 0, 0, 0), resultMergeFile2);
        assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 2, 0, 0, 2), resultMergeFile3);
    }

    @Test
    public void testLoad1ImpreciseSVVariants() throws Exception {
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        StudyMetadata sm = mm.createStudy("s1");
        List<Variant> variants = readVariants(sm, "/variant-test-sv.vcf", 1);
        List<Variant> variants2 = readVariants(sm, "/variant-test-sv.vcf", 2, "_2");

        int fid1 = mm.getFileId(sm.getId(), getFileName(1));
        int fid2 = mm.getFileId(sm.getId(), getFileName(2));

        MongoDBVariantWriteResult result = stageVariants(sm.getId(), variants, fid1);
        result = mergeVariants(sm, fid1, result);
        assertEqualsResult(new MongoDBVariantWriteResult(25, 0, 0, 0, 0, 0), result);

        result = stageVariants(sm.getId(), variants2, fid2);
        result = mergeVariants(sm, fid2, result);
        assertEqualsResult(new MongoDBVariantWriteResult(0, 25, 0, 0, 0, 0), result);

    }

    @Test
    public void testLoad2ImpreciseSVVariants() throws Exception {
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        StudyMetadata sm = mm.createStudy("s1");
        List<Variant> variants = readVariants(sm, "/variant-test-sv.vcf", 1);

        int fid1 = mm.getFileId(sm.getId(), getFileName(1));

        MongoDBVariantWriteResult result = stageVariants(sm.getId(), variants, fid1);
        result = mergeVariants(sm, fid1, result);
        assertEqualsResult(new MongoDBVariantWriteResult(25, 0, 0, 0, 0, 0), result);

        // Despite there are overlapping variants between the two files, the number of overlapping variants MUST be 0.
        // Do not check overlaps with Structural Variants!
        List<Variant> variants2 = readVariants(sm, "/variant-test-sv_2.vcf", 2);
        int fid2 = mm.getFileId(sm.getId(), getFileName(2));
        result = stageVariants(sm.getId(), variants2, fid2);
        result = mergeVariants(sm, fid2, result);
        assertEqualsResult(new MongoDBVariantWriteResult(8, 11, 12, 0, 0, 0), result);

    }

    protected List<Variant> readVariants(StudyMetadata sm, String fileName, Integer fileId) throws StorageEngineException {
        return readVariants(sm, fileName, fileId, "");
    }

    protected List<Variant> readVariants(StudyMetadata sm, String fileName, Integer fileId, String sampleSuffix)
            throws StorageEngineException {
        FullVcfCodec codec = new FullVcfCodec();
        LineIterator lineIterator = codec.makeSourceFromStream(getClass().getResourceAsStream(fileName));
        VCFHeader header = (VCFHeader) codec.readActualHeader(lineIterator);
        VariantNormalizer normalizer = new VariantNormalizer().configure(header);
        VariantFileMetadata file = new VariantFileMetadata(fileId.toString(), "file");
        VariantStudyMetadata studyMeta = file.toVariantStudyMetadata(String.valueOf(sm.getId()));

        VariantVcfHtsjdkReader reader = new VariantVcfHtsjdkReader(
                getClass().getResourceAsStream(fileName), studyMeta, normalizer);
        reader.open();
        reader.pre();
        List<Variant> variants = reader.read(1000000);
        reader.post();
        reader.close();

        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        sm.getAttributes().append(DEFAULT_GENOTYPE.key(), defaultGenotype);
        mm.unsecureUpdateStudyMetadata(sm);

        List<String> sampleNames = new ArrayList<>();
        for (String sample : file.getSampleIds()) {
            sampleNames.add(sample + sampleSuffix);
        }
        int registeredFileId = mm.registerFile(sm.getId(), getFileName(fileId), sampleNames);

        LinkedHashMap<String, Integer> samplesPosition = new LinkedHashMap<>();
        for (String sampleName : sampleNames) {
            samplesPosition.put(sampleName, samplesPosition.size());
        }

        for (Variant variant : variants) {
            variant.getStudies().get(0).setSortedSamplesPosition(samplesPosition);
            // Update file ID in variants to match the registered file ID
            variant.getStudies().get(0).getFiles().get(0)
                    .setFileId(String.valueOf(registeredFileId));
        }

        return variants;
    }

    public void assertEqualsResult(MongoDBVariantWriteResult expected, MongoDBVariantWriteResult result) {
        result.setExistingVariantsNanoTime(0).setFillGapsNanoTime(0).setNewVariantsNanoTime(0).setGenotypes(Collections.emptySet());

        expected.setUpdatedMissingVariants(0);
        if (ignoreOverlappingVariants) {
            // If ignoring overlapping variants, there wont be overlapped variants!
            expected.setOverlappedVariants(0);
        }
        assertEquals(expected, result);
    }

    public static String getFileName(Integer fileId) {
        return fileId + "_file.vcf";
    }

}
