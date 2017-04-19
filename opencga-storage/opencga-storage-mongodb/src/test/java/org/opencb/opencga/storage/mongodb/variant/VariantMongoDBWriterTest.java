/*
 * Copyright 2015-2016 OpenCB
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

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
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
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_GENOTYPE;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class VariantMongoDBWriterTest implements MongoDBVariantStorageTest {

    private static String inputFile;
    private static MongoDBVariantStorageEngine variantStorageManager;
    private VariantSource source1, source2, source3;
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

    @Before
    public void setUp() throws Exception {
        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
        stderr.setThreshold(Level.toLevel("debug"));

        inputFile = VariantStorageBaseTest.getResourceUri("variant-test-file.vcf.gz").getPath();

        clearDB(VariantStorageBaseTest.DB_NAME);
        variantStorageManager = getVariantStorageEngine();

        source1 = new VariantSource(getFileName(fileId1), fileId1.toString(), studyId1.toString(), studyName1);
        studyConfiguration = new StudyConfiguration(studyId1, studyName1);
        studyConfiguration.getSampleIds().put("NA19600", 1);
        studyConfiguration.getSampleIds().put("NA19660", 2);
        studyConfiguration.getSampleIds().put("NA19661", 3);
        studyConfiguration.getSampleIds().put("NA19685", 4);
        file1SampleIds = new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4));
        studyConfiguration.getFileIds().put(getFileName(fileId1), fileId1);
        studyConfiguration.getSamplesInFiles().put(fileId1, file1SampleIds);

        source2 = new VariantSource(getFileName(fileId2), fileId2.toString(), studyId2.toString(), studyName2);
        studyConfiguration2 = new StudyConfiguration(studyId2, studyName2);
        studyConfiguration2.getSampleIds().put("NA19600", 1);
        studyConfiguration2.getSampleIds().put("NA19660", 2);
        studyConfiguration2.getSampleIds().put("NA19661", 3);
        studyConfiguration2.getSampleIds().put("NA19685", 4);
        file2SampleIds = new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4));
        studyConfiguration2.getFileIds().put(getFileName(fileId2), fileId2);
        studyConfiguration2.getSamplesInFiles().put(fileId2, file2SampleIds);

        source3 = new VariantSource(getFileName(fileId3), fileId3.toString(), studyId2.toString(), studyName2);
        studyConfiguration2.getSampleIds().put("NA00001.X", 5);
        studyConfiguration2.getSampleIds().put("NA00002.X", 6);
        studyConfiguration2.getSampleIds().put("NA00003.X", 7);
        studyConfiguration2.getSampleIds().put("NA00004.X", 8);
        file3SampleIds = new LinkedHashSet<>(Arrays.asList(5, 6, 7, 8));
        studyConfiguration2.getFileIds().put(source3.getFileName(), fileId3);
        studyConfiguration2.getSamplesInFiles().put(fileId3, file3SampleIds);

        dbAdaptor = variantStorageManager.getDBAdaptor();
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
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));

        assertEquals(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), clearTime(loadFile1()));
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(3, allVariants.size());

        assertEquals(new MongoDBVariantWriteResult(1, 1, 0, 0, 0, 0), clearTime(loadFile2()));
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(4, allVariants.size());

        assertEquals(new MongoDBVariantWriteResult(1, 2, 1, 0, 0, 0), clearTime(loadFile3()));
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(5, allVariants.size());

        checkLoadedVariants(allVariants);

    }

    @Test
    public void testInsertMultiFilesMultiMerge() throws StorageEngineException {
        List<Variant> allVariants;
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));

        assertEquals(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), clearTime(loadFile1()));
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(3, allVariants.size());

        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();
        writeResult.merge(stageVariants(studyConfiguration2, createFile2Variants(), fileId2));
        writeResult.merge(stageVariants(studyConfiguration2, createFile3Variants(), fileId3));
        writeResult = mergeVariants(studyConfiguration2, Arrays.asList(fileId2, fileId3), writeResult, Collections.emptyList());
        assertEquals(new MongoDBVariantWriteResult(2, 2, 0, 0, 0, 0), clearTime(writeResult));
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
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
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));

        int i = 1;
        for (String chr : Arrays.asList("1", "2", "3")) {
            Query query = new Query(VariantQueryParam.REGION.key(), chr);

            assertEquals(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), clearTime(loadFile1(chr, i++, Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            assertEquals(3, allVariants.size());

            assertEquals(new MongoDBVariantWriteResult(1, 1, 0, 0, 0, 0), clearTime(loadFile2(chr, i++, Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            assertEquals(4, allVariants.size());

            assertEquals(new MongoDBVariantWriteResult(1, 2, 1, 0, 0, 0), clearTime(loadFile3(chr, i++, Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
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
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));


        int i = 1;
        List<String> chromosomes = Arrays.asList("1", "2", "3", "4");
        Map<String, int[]> mapFileIds = new HashMap<>();
        for (String chr : chromosomes) {
            mapFileIds.put(chr, new int[]{i++, i++, i++});
            Query query = new Query(VariantQueryParam.REGION.key(), chr);

            assertEquals(new MongoDBVariantWriteResult(2, 0, 0, 0, 0, 0), clearTime(loadFile2(chr, mapFileIds.get(chr)[1], Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            assertEquals(2, allVariants.size());

            assertEquals(new MongoDBVariantWriteResult(2, 1, 1, 0, 0, 0), clearTime(loadFile3(chr, mapFileIds.get(chr)[2], Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            assertEquals(4, allVariants.size());

        }

        for (String chr : chromosomes) {
            Query query = new Query(VariantQueryParam.REGION.key(), chr);

            assertEquals(new MongoDBVariantWriteResult(1, 2, 0, 0, 0, 0), clearTime(loadFile1(chr, mapFileIds.get(chr)[0], Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            assertEquals(5, allVariants.size());

        }

        for (String chr : chromosomes) {
            Query query = new Query(VariantQueryParam.REGION.key(), chr);
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
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
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));


        int i = 1;
        List<String> chromosomes = Arrays.asList("1", "2", "X", "3", "5", "4");
//        List<String> chromosomes = Arrays.asList("4", "3", "2", "1");
        Map<String, int[]> mapFileIds = new HashMap<>();
        for (String chr : chromosomes) {
            mapFileIds.put(chr, new int[]{i++, i++, i++});
            Query query = new Query(VariantQueryParam.REGION.key(), chr);

            assertEquals(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), clearTime(loadFile1(chr, mapFileIds.get(chr)[0], Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            assertEquals(3, allVariants.size());

        }

        for (String chr : chromosomes) {
            Query query = new Query(VariantQueryParam.REGION.key(), chr);

            assertEquals(new MongoDBVariantWriteResult(1, 1, 0, 0, 0, 0), clearTime(loadFile2(chr, mapFileIds.get(chr)[1], Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            assertEquals(4, allVariants.size());

            assertEquals(new MongoDBVariantWriteResult(1, 2, 1, 0, 0, 0), clearTime(loadFile3(chr, mapFileIds.get(chr)[2], Collections.singletonList(chr))));
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            assertEquals(5, allVariants.size());
        }

        for (String chr : chromosomes) {
            Query query = new Query(VariantQueryParam.REGION.key(), chr);
            allVariants = dbAdaptor.get(query, new QueryOptions("sort", true)).getResult();
            checkLoadedVariants(allVariants, mapFileIds.get(chr));
        }
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
            StudyEntry study = variant.getStudy(studyConfiguration.getStudyName());
            assertTrue(study.getSamplesName().contains(sampleName));
            assertEquals("FileId=" + fileId + " Field=" + field + " Sample=" + sampleName + " (" + sampleId + ")", valueProvider.apply(sampleId),
                    study.getSampleData(sampleName, field));
        });
    }

    public MongoDBVariantWriteResult loadFile1() throws StorageEngineException {
        return loadFile1("X", Integer.parseInt(source1.getFileId()), Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile1(String chromosome, Integer fileId, List<String> chromosomes) throws StorageEngineException {
        studyConfiguration.getFileIds().putIfAbsent(getFileName(fileId), fileId);
        studyConfiguration.getSamplesInFiles().putIfAbsent(fileId, file1SampleIds);
        System.out.println("chromosome = " + chromosome);
        System.out.println("fileId = " + fileId);
        System.out.println("samples = " + file1SampleIds.stream().map(i -> studyConfiguration.getSampleIds().inverse().get(i)).collect(Collectors.toList()) + " : " + file1SampleIds);
        return loadFile(studyConfiguration, createFile1Variants(chromosome, fileId.toString(), Integer.toString(studyConfiguration.getStudyId())), fileId, chromosomes);
    }

    public MongoDBVariantWriteResult loadFile2() throws StorageEngineException {
        return loadFile2("X", Integer.parseInt(source2.getFileId()), Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile2(String chromosome, Integer fileId, List<String> chromosomes) throws StorageEngineException {
        studyConfiguration2.getFileIds().putIfAbsent(getFileName(fileId), fileId);
        studyConfiguration2.getSamplesInFiles().putIfAbsent(fileId, file2SampleIds);
        System.out.println("chromosome = " + chromosome);
        System.out.println("fileId = " + fileId);
        System.out.println("samples = " + file2SampleIds.stream().map(i -> studyConfiguration2.getSampleIds().inverse().get(i)).collect(Collectors.toList()) + " : " + file2SampleIds);
        return loadFile(studyConfiguration2, createFile2Variants(chromosome, fileId.toString(), source2.getStudyId()), fileId, chromosomes);
    }

    public MongoDBVariantWriteResult loadFile3() throws StorageEngineException {
        return loadFile3("X", Integer.parseInt(source3.getFileId()), Collections.emptyList());
    }

    public MongoDBVariantWriteResult loadFile3(String chromosome, Integer fileId, List<String> chromosomes) throws StorageEngineException {
        studyConfiguration2.getFileIds().putIfAbsent(getFileName(fileId), fileId);
        studyConfiguration2.getSamplesInFiles().putIfAbsent(fileId, file3SampleIds);
        System.out.println("chromosome = " + chromosome);
        System.out.println("fileId = " + fileId);
        System.out.println("samples = " + file3SampleIds.stream().map(i -> studyConfiguration2.getSampleIds().inverse().get(i)).collect(Collectors.toList()) + " : " + file3SampleIds);
        return loadFile(studyConfiguration2, createFile3Variants(chromosome, fileId.toString(), source3.getStudyId()), fileId, chromosomes);
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
        MongoDBCollection stage = dbAdaptor.getStageCollection();
        MongoDBVariantStageLoader variantStageLoader = new MongoDBVariantStageLoader(stage, studyConfiguration.getStudyId(), fileId, false);
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
        MongoDBCollection stage = dbAdaptor.getStageCollection();
        MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();
        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(stage, studyConfiguration.getStudyId(), chromosomes);
        MongoDBVariantMerger dbMerger = new MongoDBVariantMerger(dbAdaptor, studyConfiguration, fileIds,
                studyConfiguration.getIndexedFiles(), false, false);
        boolean cleanWhileLoading = true;
        boolean resume = false;
        MongoDBVariantMergeLoader variantLoader = new MongoDBVariantMergeLoader(variantsCollection, dbAdaptor.getStageCollection(),
                studyConfiguration.getStudyId(), fileIds, resume, cleanWhileLoading, null);

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

        long cleanedDocuments = MongoDBVariantStageLoader.cleanStageCollection(stage, studyConfiguration.getStudyId(), fileIds, null, null);
        if (cleanWhileLoading) {
            assertEquals(0, cleanedDocuments);
        } else {
            assertNotEquals(0, cleanedDocuments);
        }
        studyConfiguration.getIndexedFiles().addAll(fileIds);
        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
        return variantLoader.getResult().setSkippedVariants(stageWriteResult.getSkippedVariants());
    }

    public List<Variant> createFile1Variants() {
        return createFile1Variants("X", source1.getFileId(), source1.getStudyId());
    }
    public List<Variant> createFile2Variants() {
        return createFile2Variants("X", source2.getFileId(), source2.getStudyId());
    }
    public List<Variant> createFile3Variants() {
        return createFile3Variants("X", source3.getFileId(), source3.getStudyId());
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile1Variants(String chromosome, String fileId, String studyId) {

        Variant variant;
        StudyEntry sourceEntry;
        List<Variant> variants = new LinkedList<>();
        variant = new Variant(chromosome, 999, 999, "A", "C");
        sourceEntry = new StudyEntry(fileId, studyId);
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant(chromosome, 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(fileId, studyId);
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant(chromosome, 1002, 1002, "A", "C");
        sourceEntry = new StudyEntry(fileId, studyId);
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "0/1").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "0/0").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "1/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "0/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        return variants;
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile2Variants(String chromosome, String fileId, String studyId) {
        Variant variant;
        StudyEntry sourceEntry;
        List<Variant> variants = new LinkedList<>();

        variant = new Variant(chromosome, 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(fileId, studyId);
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "1").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "2").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "3").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "4").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant(chromosome, 1004, 1004, "A", "C");
        sourceEntry = new StudyEntry(fileId, studyId);
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "0/1").append("DP", "1").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "0/0").append("DP", "2").append("GQX", ".")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "1/0").append("DP", "3").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "0/0").append("DP", "4").append("GQX", "..")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        return variants;
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile3Variants(String chromosome, String fileId, String studyId) {

        Variant variant;
        StudyEntry sourceEntry;
        List<Variant> variants = new LinkedList<>();

        variant = new Variant(chromosome, 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(fileId, studyId);
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant(chromosome, 1002, 1002, "A", "C");
        sourceEntry = new StudyEntry(fileId, studyId);
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant(chromosome, 1006, 1006, "A", "C");
        sourceEntry = new StudyEntry(fileId, studyId);
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        return variants;
    }

    @Test
    public void testInsertSameVariantTwice() throws StorageEngineException {

        String chromosome = "1";
        loadFile1(chromosome, fileId1, Collections.singletonList(chromosome));
        loadFile2(chromosome, fileId2, Collections.singletonList(chromosome));
        Integer fileId = fileId3;
        studyConfiguration2.getFileIds().putIfAbsent(getFileName(fileId), fileId);
        studyConfiguration2.getSamplesInFiles().putIfAbsent(fileId, file3SampleIds);

        List<Variant> file3Variants = createFile3Variants(chromosome, fileId.toString(), source3.getStudyId());
        file3Variants.add(file3Variants.get(2));

        MongoDBVariantWriteResult result = loadFile(studyConfiguration2, file3Variants, fileId, Collections.singletonList(chromosome));
        assertEquals(new MongoDBVariantWriteResult(0, 2, 1, 0, 0, 2), clearTime(result));
    }

    public MongoDBVariantWriteResult clearTime(MongoDBVariantWriteResult writeResult) {
        return writeResult.setExistingVariantsNanoTime(0).setFillGapsNanoTime(0).setNewVariantsNanoTime(0);
    }

    public static String getFileName(Integer fileId) {
        return fileId + "_file.vcf";
    }

}
