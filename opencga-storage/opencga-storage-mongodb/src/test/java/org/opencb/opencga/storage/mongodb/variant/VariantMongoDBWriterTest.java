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
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
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
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_FIELD;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
@RunWith(Parameterized.class)
public class VariantMongoDBWriterTest implements MongoDBVariantStorageTest {

    private Query query;
    private static final QueryOptions QUERY_OPTIONS = new QueryOptions(QueryOptions.SORT, true);
    private static String inputFile;
    private static MongoDBVariantStorageEngine variantStorageManager;
    private VariantStudyMetadata metadata1, metadata2, metadata3;
    private StudyConfiguration studyConfiguration, studyConfiguration2;
    private final Integer fileId1 = 10000;
    private final Integer fileId2 = 20000;
    private final Integer fileId3 = 30000;
    private Integer studyId1 = 1;
    private Integer studyId2 = 2;
    private String studyName1 = "Study 1";
    private String studyName2 = "Study 2";
    private VariantMongoDBAdaptor dbAdaptor;
    private LinkedHashSet<Integer> file1SampleIds;
    private LinkedHashSet<Integer> file2SampleIds;
    private LinkedHashSet<Integer> file3SampleIds;

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
        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
        stderr.setThreshold(Level.toLevel("debug"));

        inputFile = VariantStorageBaseTest.getResourceUri("variant-test-file.vcf.gz").getPath();

        clearDB(VariantStorageBaseTest.DB_NAME);
        variantStorageManager = getVariantStorageEngine();

        metadata1 = new VariantFileMetadata(fileId1.toString(), getFileName(fileId1)).toVariantStudyMetadata(studyId1.toString());
        studyConfiguration = new StudyConfiguration(studyId1, studyName1);
        studyConfiguration.getAttributes().append(DEFAULT_GENOTYPE.key(), defaultGenotype);
        studyConfiguration.getSampleIds().put("NA19600", 1);
        studyConfiguration.getSampleIds().put("NA19660", 2);
        studyConfiguration.getSampleIds().put("NA19661", 3);
        studyConfiguration.getSampleIds().put("NA19685", 4);
        file1SampleIds = new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4));
        studyConfiguration.getFileIds().put(getFileName(fileId1), fileId1);
        studyConfiguration.getSamplesInFiles().put(fileId1, file1SampleIds);

        metadata2 = new VariantFileMetadata(fileId2.toString(), getFileName(fileId2)).toVariantStudyMetadata(studyId2.toString());
        studyConfiguration2 = new StudyConfiguration(studyId2, studyName2);
        studyConfiguration2.getAttributes().append(DEFAULT_GENOTYPE.key(), defaultGenotype);
        studyConfiguration2.getSampleIds().put("NA19600", 1);
        studyConfiguration2.getSampleIds().put("NA19660", 2);
        studyConfiguration2.getSampleIds().put("NA19661", 3);
        studyConfiguration2.getSampleIds().put("NA19685", 4);
        file2SampleIds = new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4));
        studyConfiguration2.getFileIds().put(getFileName(fileId2), fileId2);
        studyConfiguration2.getSamplesInFiles().put(fileId2, file2SampleIds);

        metadata3 = new VariantFileMetadata(fileId3.toString(), getFileName(fileId3)).toVariantStudyMetadata(studyId2.toString());
        studyConfiguration2.getSampleIds().put("NA00001.X", 5);
        studyConfiguration2.getSampleIds().put("NA00002.X", 6);
        studyConfiguration2.getSampleIds().put("NA00003.X", 7);
        studyConfiguration2.getSampleIds().put("NA00004.X", 8);
        file3SampleIds = new LinkedHashSet<>(Arrays.asList(5, 6, 7, 8));
        studyConfiguration2.getFileIds().put(getFileName(fileId3), fileId3);
        studyConfiguration2.getSamplesInFiles().put(fileId3, file3SampleIds);

        dbAdaptor = variantStorageManager.getDBAdaptor();
        query = new Query();
        if (defaultGenotype.equals("0/0")) {
            query.append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "?/?");
        }
    }


    @After
    public void shutdown() throws Exception {
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
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));

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
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));

        assertEqualsResult(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), loadFile1());
        allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
        assertEquals(3, allVariants.size());

        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();
        writeResult.merge(stageVariants(studyConfiguration2, createFile2Variants(), fileId2));
        writeResult.merge(stageVariants(studyConfiguration2, createFile3Variants(), fileId3));
        writeResult = mergeVariants(studyConfiguration2, Arrays.asList(fileId2, fileId3), writeResult, Collections.emptyList());
        assertEqualsResult(new MongoDBVariantWriteResult(2, 2, 0, 0, 0, 0), writeResult);
        allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
        assertEquals(5, allVariants.size());

        checkLoadedVariants(allVariants);

    }

    /**
     * Insert variants chromosome by chromosome
     *
     * @throws StorageEngineException
     */
    @Test
    public void testInsertMultiFilesMultipleRegions() throws StorageEngineException {
        List<Variant> allVariants;
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));

        int i = 1;
        for (String chr : Arrays.asList("1", "2", "3")) {
            Query query = getQuery(chr);

            MongoDBVariantWriteResult writeResult2 = loadFile1(chr, i++, Collections.singletonList(chr));
            assertEqualsResult(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), writeResult2);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(3, allVariants.size());

            MongoDBVariantWriteResult writeResult1 = loadFile2(chr, i++, Collections.singletonList(chr));
            assertEqualsResult(new MongoDBVariantWriteResult(1, 1, 0, 0, 0, 0), writeResult1);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(4, allVariants.size());

            MongoDBVariantWriteResult writeResult = loadFile3(chr, i++, Collections.singletonList(chr));
            assertEqualsResult(new MongoDBVariantWriteResult(1, 2, 1, 0, 0, 0), writeResult);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(5, allVariants.size());

            checkLoadedVariants(allVariants);
        }
    }

    /**
     * Insert variants study by study
     *
     * @throws StorageEngineException
     */
    @Test
    public void testInsertMultiFilesMultipleRegionsStudyByStudy() throws StorageEngineException {
        List<Variant> allVariants;
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));


        int i = 1;
        List<String> chromosomes = Arrays.asList("1", "2", "3", "4");
        Map<String, int[]> mapFileIds = new HashMap<>();
        for (String chr : chromosomes) {
            mapFileIds.put(chr, new int[]{i++, i++, i++});
            Query query = getQuery(chr);

            assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 0, 0, 0, 0), loadFile2(chr, mapFileIds.get(chr)[1], Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(2, allVariants.size());

            assertEqualsResult(new MongoDBVariantWriteResult(2, 1, 1, 0, 0, 0), loadFile3(chr, mapFileIds.get(chr)[2], Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(4, allVariants.size());

        }

        for (String chr : chromosomes) {
            Query query = getQuery(chr);

            assertEqualsResult(new MongoDBVariantWriteResult(1, 2, 0, 0, 0, 0), loadFile1(chr, mapFileIds.get(chr)[0], Collections.singletonList(chr)));
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
     * @throws StorageEngineException
     */
    @Test
    public void testInsertMultiFilesMultipleRegionsStudyByStudy2() throws StorageEngineException {
        List<Variant> allVariants;
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));


        int i = 1;
        List<String> chromosomes = Arrays.asList("1", "2", "X", "3", "5", "4");
//        List<String> chromosomes = Arrays.asList("4", "3", "2", "1");
        Map<String, int[]> mapFileIds = new HashMap<>();
        for (String chr : chromosomes) {
            mapFileIds.put(chr, new int[]{i++, i++, i++});
            Query query = getQuery(chr);

            assertEqualsResult(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), loadFile1(chr, mapFileIds.get(chr)[0], Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(3, allVariants.size());

        }

        for (String chr : chromosomes) {
            Query query = getQuery(chr);

            assertEqualsResult(new MongoDBVariantWriteResult(1, 1, 0, 0, 0, 0), loadFile2(chr, mapFileIds.get(chr)[1], Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(4, allVariants.size());

            assertEqualsResult(new MongoDBVariantWriteResult(1, 2, 1, 0, 0, 0), loadFile3(chr, mapFileIds.get(chr)[2], Collections.singletonList(chr)));
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            assertEquals(5, allVariants.size());
        }

        for (String chr : chromosomes) {
            Query query = getQuery(chr);
            allVariants = dbAdaptor.get(query, QUERY_OPTIONS).getResults();
            checkLoadedVariants(allVariants, mapFileIds.get(chr));
        }
    }

    public Query getQuery(String chr) {
        return new Query(this.query).append(VariantQueryParam.REGION.key(), chr);
    }

    public void checkLoadedVariants(List<Variant> allVariants) {
        checkLoadedVariants(allVariants, new int[]{fileId1, fileId2, fileId3});
    }

    public void checkLoadedVariants(List<Variant> allVariants, int[] fileIds) {
        Variant variant;
        variant = allVariants.get(0);
        assertEquals(999, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName1), variant.getStudiesMap().keySet());

        variant = allVariants.get(1);
        assertEquals(1000, variant.getStart().longValue());
        assertEquals(new HashSet<>(Arrays.asList(studyName1, studyName2)), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration, fileIds[0], (sampleId) -> Integer.toString(sampleId + 10), "DP");

        variant = allVariants.get(2);
        assertEquals(1002, variant.getStart().longValue());
        assertEquals(new HashSet<>(Arrays.asList(studyName1, studyName2)), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration, fileIds[0], (sampleId) -> Integer.toString(sampleId + 10), "DP");
        checkSampleData(variant, studyConfiguration2, fileIds[1], (sampleId) -> UNKNOWN_FIELD, "DP");
        checkSampleData(variant, studyConfiguration2, fileIds[1], (sampleId) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyConfiguration2, fileIds[2], Object::toString, "DP");

        variant = allVariants.get(3);
        assertEquals(1004, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName2), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration2, fileIds[1], Object::toString, "DP");
        checkSampleData(variant, studyConfiguration2, fileIds[2], (sampleId) -> UNKNOWN_FIELD, "DP");
        checkSampleData(variant, studyConfiguration2, fileIds[2], (sampleId) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyConfiguration2, fileIds[1], (sampleId) -> sampleId % 2 == 0 ? UNKNOWN_FIELD : "0.7", "GQX");
        checkSampleData(variant, studyConfiguration2, fileIds[2], (sampleId) -> UNKNOWN_FIELD, "GQX");

        variant = allVariants.get(4);
        assertEquals(1006, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName2), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration2, fileIds[1], (sampleId) -> UNKNOWN_FIELD, "DP");
        checkSampleData(variant, studyConfiguration2, fileIds[1], (sampleId) -> UNKNOWN_FIELD, "GQX");
        checkSampleData(variant, studyConfiguration2, fileIds[1], (sampleId) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyConfiguration2, fileIds[2], Object::toString, "DP");
        checkSampleData(variant, studyConfiguration2, fileIds[2], (sampleId) -> "0.7", "GQX");
    }


    public void checkSampleData(Variant variant, StudyConfiguration studyConfiguration, Integer fileId, Function<Integer, String>
            valueProvider, String field) {
        assertTrue(studyConfiguration.getFileIds().values().contains(fileId));
        studyConfiguration.getSamplesInFiles().get(fileId).forEach((sampleId) ->
        {
            String sampleName = studyConfiguration.getSampleIds().inverse().get(sampleId);
            StudyEntry study = variant.getStudy(studyConfiguration.getName());
            assertTrue(study.getSamplesName().contains(sampleName));
            assertEquals("Variant=" + variant + " StudyId=" + studyConfiguration.getId() + " FileId=" + fileId + " Field=" + field + " Sample=" + sampleName + " (" + sampleId + ")\n"+variant.toJson(),
                    valueProvider.apply(sampleId), study.getSampleData(sampleName, field));
        });
    }

    public MongoDBVariantWriteResult loadFile1() throws StorageEngineException {
        return loadFile1("X", Integer.parseInt(metadata1.getFiles().get(0).getId()), Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile1(String chromosome, Integer fileId, List<String> chromosomes) throws StorageEngineException {
        studyConfiguration.getFileIds().putIfAbsent(getFileName(fileId), fileId);
        studyConfiguration.getSamplesInFiles().putIfAbsent(fileId, file1SampleIds);
        System.out.println("chromosome = " + chromosome);
        System.out.println("fileId = " + fileId);
        System.out.println("samples = " + file1SampleIds.stream().map(i -> studyConfiguration.getSampleIds().inverse().get(i)).collect(Collectors.toList()) + " : " + file1SampleIds);
        return loadFile(studyConfiguration, createFile1Variants(chromosome, fileId.toString(), Integer.toString(studyConfiguration.getId())), fileId, chromosomes);
    }

    public MongoDBVariantWriteResult loadFile2() throws StorageEngineException {
        return loadFile2("X", Integer.parseInt(metadata2.getFiles().get(0).getId()), Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile2(String chromosome, Integer fileId, List<String> chromosomes) throws StorageEngineException {
        studyConfiguration2.getFileIds().putIfAbsent(getFileName(fileId), fileId);
        studyConfiguration2.getSamplesInFiles().putIfAbsent(fileId, file2SampleIds);
        System.out.println("chromosome = " + chromosome);
        System.out.println("fileId = " + fileId);
        System.out.println("samples = " + file2SampleIds.stream().map(i -> studyConfiguration2.getSampleIds().inverse().get(i)).collect(Collectors.toList()) + " : " + file2SampleIds);
        return loadFile(studyConfiguration2, createFile2Variants(chromosome, fileId.toString(), metadata2.getId()), fileId, chromosomes);
    }

    public MongoDBVariantWriteResult loadFile3() throws StorageEngineException {
        return loadFile3("X", Integer.parseInt(metadata3.getFiles().get(0).getId()), Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile3(String chromosome, Integer fileId, List<String> chromosomes) throws StorageEngineException {
        studyConfiguration2.getFileIds().putIfAbsent(getFileName(fileId), fileId);
        studyConfiguration2.getSamplesInFiles().putIfAbsent(fileId, file3SampleIds);
        System.out.println("chromosome = " + chromosome);
        System.out.println("fileId = " + fileId);
        System.out.println("samples = " + file3SampleIds.stream().map(i -> studyConfiguration2.getSampleIds().inverse().get(i)).collect(Collectors.toList()) + " : " + file3SampleIds);
        return loadFile(studyConfiguration2, createFile3Variants(chromosome, fileId.toString(), metadata3.getId()), fileId, chromosomes);
    }

    public MongoDBVariantWriteResult loadFile(StudyConfiguration studyConfiguration, List<Variant> variants, int fileId)
            throws StorageEngineException {
        return loadFile(studyConfiguration, variants, fileId, Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile(StudyConfiguration studyConfiguration, List<Variant> variants, int fileId, List<String> chromosomes)
            throws StorageEngineException {
//        return loadFileOld(studyConfiguration, variants, fileId);
        MongoDBVariantWriteResult stageWriteResult = stageVariants(studyConfiguration, variants, fileId);
        return mergeVariants(studyConfiguration, Collections.singletonList(fileId), stageWriteResult, chromosomes);
    }

    public MongoDBVariantWriteResult stageVariants(StudyConfiguration studyConfiguration, List<Variant> variants, int fileId) {
        MongoDBCollection stage = dbAdaptor.getStageCollection(studyConfiguration.getId());
        MongoDBVariantStageLoader variantStageLoader = new MongoDBVariantStageLoader(stage, studyConfiguration.getId(), fileId, false);
        MongoDBVariantStageConverterTask converterTask = new MongoDBVariantStageConverterTask(null);

        variantStageLoader.write(converterTask.apply(variants));

        return variantStageLoader.getWriteResult();
    }

    public MongoDBVariantWriteResult mergeVariants(StudyConfiguration studyConfiguration, int fileId,
                                                   MongoDBVariantWriteResult stageWriteResult) {

        return mergeVariants(studyConfiguration, Collections.singletonList(fileId), stageWriteResult, Collections.emptyList());
    }

    public MongoDBVariantWriteResult mergeVariants(StudyConfiguration studyConfiguration, List<Integer> fileIds,
                                                   MongoDBVariantWriteResult stageWriteResult, List<String> chromosomes) {
        MongoDBCollection stage = dbAdaptor.getStageCollection(studyConfiguration.getId());
        MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();
        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(stage, studyConfiguration.getId(), chromosomes);
        MongoDBVariantMerger dbMerger = new MongoDBVariantMerger(dbAdaptor, studyConfiguration, fileIds, false, ignoreOverlappingVariants, 1);
        boolean resume = false;
        MongoDBVariantMergeLoader variantLoader = new MongoDBVariantMergeLoader(variantsCollection, dbAdaptor.getStageCollection(studyConfiguration.getId()),
                dbAdaptor.getStudiesCollection(), studyConfiguration, fileIds, resume, cleanWhileLoading, null);

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

        long cleanedDocuments = MongoDBVariantStageLoader.cleanStageCollection(stage, studyConfiguration.getId(), fileIds, null, null);
        if (cleanWhileLoading) {
            assertEquals(0, cleanedDocuments);
        } else {
            assertNotEquals(0, cleanedDocuments);
        }
        studyConfiguration.getIndexedFiles().addAll(fileIds);
        dbAdaptor.getMetadataManager().updateStudyConfiguration(studyConfiguration, null);
        return variantLoader.getResult().setSkippedVariants(stageWriteResult != null ? stageWriteResult.getSkippedVariants() : 0);
    }

    public List<Variant> createFile1Variants() {
        return createFile1Variants("X", metadata1.getFiles().get(0).getId(), metadata1.getId());
    }
    public List<Variant> createFile2Variants() {
        return createFile2Variants("X", metadata2.getFiles().get(0).getId(), metadata2.getId());
    }
    public List<Variant> createFile3Variants() {
        return createFile3Variants("X", metadata3.getFiles().get(0).getId(), metadata3.getId());
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

        MongoDBVariantWriteResult result = loadFile(studyConfiguration2, file3Variants, fileId3);
        assertEqualsResult(new MongoDBVariantWriteResult(0, 2, 1, 0, 0, 2), result);
    }

    @Test
    public void testDuplicatedVariantOnlyOneFile_mergeAtSameTime() throws StorageEngineException {
        List<Variant> file2Variants = createFile2Variants();
        List<Variant> file3Variants = createFile3Variants();
        file3Variants.add(file3Variants.get(0));
        assertThat(file3Variants.get(0), VariantMatchers.overlaps(file2Variants.get(0)));

        stageVariants(studyConfiguration2, file3Variants, fileId3);
        stageVariants(studyConfiguration2, file2Variants, fileId2);
        MongoDBVariantWriteResult result = mergeVariants(studyConfiguration2, Arrays.asList(fileId2, fileId3), null, Collections.emptyList());
        assertEqualsResult(new MongoDBVariantWriteResult(4, 0, 0, 0, 0, 2), result);
    }

    @Test
    public void testDuplicatedVariantOnlyOneFile_mergeDuplicatedFirst() throws StorageEngineException {
        List<Variant> file2Variants = createFile2Variants();
        List<Variant> file3Variants = createFile3Variants();
        file3Variants.add(file3Variants.get(0));
        assertThat(file3Variants.get(0), VariantMatchers.overlaps(file2Variants.get(0)));

        stageVariants(studyConfiguration2, file3Variants, fileId3);
        stageVariants(studyConfiguration2, file2Variants, fileId2);
        MongoDBVariantWriteResult resultMergeFile3 = mergeVariants(studyConfiguration2, fileId3, null);
        MongoDBVariantWriteResult resultMergeFile2 = mergeVariants(studyConfiguration2, fileId2, null);
        assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 0, 0, 0, 2), resultMergeFile3);
        assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 2, 0, 0, 0), resultMergeFile2);
    }

    @Test
    public void testDuplicatedVariantOnlyOneFile_mergeDuplicatedLast() throws StorageEngineException {
        List<Variant> file2Variants = createFile2Variants();
        List<Variant> file3Variants = createFile3Variants();
        file3Variants.add(file3Variants.get(0));
        assertThat(file3Variants.get(0), VariantMatchers.overlaps(file2Variants.get(0)));

        stageVariants(studyConfiguration2, file3Variants, fileId3);
        stageVariants(studyConfiguration2, file2Variants, fileId2);
        MongoDBVariantWriteResult resultMergeFile2 = mergeVariants(studyConfiguration2, fileId2, null);
        MongoDBVariantWriteResult resultMergeFile3 = mergeVariants(studyConfiguration2, fileId3, null);
        assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 0, 0, 0, 0), resultMergeFile2);
        assertEqualsResult(new MongoDBVariantWriteResult(2, 0, 2, 0, 0, 2), resultMergeFile3);
    }

    @Test
    public void testLoad1ImpreciseSVVariants() throws Exception {
        StudyConfiguration sc = new StudyConfiguration(1, "s1");
        List<Variant> variants = readVariants(sc, "/variant-test-sv.vcf", 1);
        List<Variant> variants2 = readVariants(sc, "/variant-test-sv.vcf", 2, "_2");

        MongoDBVariantWriteResult result = stageVariants(sc, variants, 1);
        result = mergeVariants(sc, 1, result);
        assertEqualsResult(new MongoDBVariantWriteResult(23, 0, 0, 0, 0, 0), result);

        result = stageVariants(sc, variants2, 2);
        result = mergeVariants(sc, 2, result);
        assertEqualsResult(new MongoDBVariantWriteResult(0, 23, 0, 0, 0, 0), result);

    }

    @Test
    public void testLoad2ImpreciseSVVariants() throws Exception {
        StudyConfiguration sc = new StudyConfiguration(1, "s1");
        List<Variant> variants = readVariants(sc, "/variant-test-sv.vcf", 1);

        MongoDBVariantWriteResult result = stageVariants(sc, variants, 1);
        result = mergeVariants(sc, 1, result);
        assertEqualsResult(new MongoDBVariantWriteResult(23, 0, 0, 0, 0, 0), result);

        // Despite there are overlapping variants between the two files, the number of overlapping variants MUST be 0.
        // Do not check overlaps with Structural Variants!
        List<Variant> variants2 = readVariants(sc, "/variant-test-sv_2.vcf", 2);
        result = stageVariants(sc, variants2, 2);
        result = mergeVariants(sc, 2, result);
        assertEqualsResult(new MongoDBVariantWriteResult(8, 11, 12, 0, 0, 0), result);

    }

    protected List<Variant> readVariants(StudyConfiguration sc, String fileName, Integer fileId) {
        return readVariants(sc, fileName, fileId, "");
    }

    protected List<Variant> readVariants(StudyConfiguration sc, String fileName, Integer fileId, String sampleSufix) {
        FullVcfCodec codec = new FullVcfCodec();
        LineIterator lineIterator = codec.makeSourceFromStream(getClass().getResourceAsStream(fileName));
        VCFHeader header = (VCFHeader) codec.readActualHeader(lineIterator);
        VariantNormalizer normalizer = new VariantNormalizer().configure(header);
        VariantFileMetadata file = new VariantFileMetadata(fileId.toString(), "file");
        VariantStudyMetadata studyMetadata = file.toVariantStudyMetadata(String.valueOf(sc.getId()));

        VariantVcfHtsjdkReader reader = new VariantVcfHtsjdkReader(getClass().getResourceAsStream(fileName), studyMetadata, normalizer);
        reader.open();
        reader.pre();
        List<Variant> variants = reader.read(1000000);
        reader.post();
        reader.close();


        sc.getAttributes().append(DEFAULT_GENOTYPE.key(), defaultGenotype);
        LinkedHashSet<Integer> sampleIds = new LinkedHashSet<>();
        LinkedHashMap<String, Integer> samplesPosition = new LinkedHashMap<>();
        for (String sample : file.getSampleIds()) {
            sample = sample + sampleSufix;
            sc.getSampleIds().putIfAbsent(sample, sc.getSampleIds().size() + 1);
            sampleIds.add(sc.getSampleIds().get(sample));
            samplesPosition.put(sample, samplesPosition.size());
        }
        sc.getFileIds().put(getFileName(fileId), fileId);
        sc.getSamplesInFiles().put(fileId, sampleIds);

        for (Variant variant : variants) {
            variant.getStudies().get(0).setSortedSamplesPosition(samplesPosition);
        }

        return variants;
    }

    public void assertEqualsResult(MongoDBVariantWriteResult expected, MongoDBVariantWriteResult result) {
        result.setExistingVariantsNanoTime(0).setFillGapsNanoTime(0).setNewVariantsNanoTime(0).setGenotypes(Collections.emptySet());

//        if (defaultGenotype.equals(UNKNOWN_GENOTYPE)) {
//            // If defaultGenotype is the unknown, overlapping missing variants won't not be updated
//            expected.setUpdatedMissingVariants(0);
//        }
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
