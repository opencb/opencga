/*
 * Copyright 2015 OpenCB
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
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantMerger;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantStageLoader;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantStageReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_GENOTYPE;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class VariantMongoDBWriterTest implements MongoVariantStorageManagerTestUtils {

    private static String inputFile;
    private static MongoDBVariantStorageManager variantStorageManager;
    private VariantSource source1, source2, source3;
    private StudyConfiguration studyConfiguration, studyConfiguration2;
    private final Integer fileId1 = 1;
    private final Integer fileId2 = 2;
    private final Integer fileId3 = 3;
    private Integer studyId1 = 1;
    private Integer studyId2 = 2;
    private String studyName1 = "Study 1";
    private String studyName2 = "Study 2";
    private VariantMongoDBAdaptor dbAdaptor;

    @Before
    public void setUp() throws Exception {
        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
        stderr.setThreshold(Level.toLevel("debug"));

        inputFile = VariantStorageManagerTestUtils.getResourceUri("variant-test-file.vcf.gz").getPath();

        clearDB(VariantStorageManagerTestUtils.DB_NAME);
        variantStorageManager = getVariantStorageManager();

        source1 = new VariantSource(inputFile, fileId1.toString(), studyId1.toString(), studyName1);
        studyConfiguration = new StudyConfiguration(studyId1, studyName1);
        studyConfiguration.getSampleIds().put("NA19600", 1);
        studyConfiguration.getSampleIds().put("NA19660", 2);
        studyConfiguration.getSampleIds().put("NA19661", 3);
        studyConfiguration.getSampleIds().put("NA19685", 4);
        studyConfiguration.getFileIds().put(inputFile, fileId1);
        studyConfiguration.getSamplesInFiles().put(fileId1, new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4)));

        source2 = new VariantSource(inputFile, fileId2.toString(), studyId2.toString(), studyName2);
        studyConfiguration2 = new StudyConfiguration(studyId2, studyName2);
        studyConfiguration2.getSampleIds().put("NA19600", 1);
        studyConfiguration2.getSampleIds().put("NA19660", 2);
        studyConfiguration2.getSampleIds().put("NA19661", 3);
        studyConfiguration2.getSampleIds().put("NA19685", 4);
        studyConfiguration2.getFileIds().put(inputFile, fileId2);
        studyConfiguration2.getSamplesInFiles().put(fileId2, new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4)));

        source3 = new VariantSource("unreadl.vcf", fileId3.toString(), studyId2.toString(), studyName2);
        studyConfiguration2.getSampleIds().put("NA00001.X", 5);
        studyConfiguration2.getSampleIds().put("NA00002.X", 6);
        studyConfiguration2.getSampleIds().put("NA00003.X", 7);
        studyConfiguration2.getSampleIds().put("NA00004.X", 8);
        studyConfiguration2.getFileIds().put(source3.getFileName(), fileId3);
        studyConfiguration2.getSamplesInFiles().put(fileId3, new LinkedHashSet<>(Arrays.asList(5, 6, 7, 8)));

        dbAdaptor = variantStorageManager.getDBAdaptor(VariantStorageManagerTestUtils.DB_NAME);
    }

    @After
    public void shutdown() throws Exception {
    }

    @Test
    public void test() throws IOException, StorageManagerException {


        VariantReader reader = new VariantVcfReader(source1, inputFile);

        List<Task<Variant>> taskList = new SortedList<>();
        List<VariantWriter> writers = new ArrayList<>();


        writers.add(new VariantMongoDBWriter(fileId1, studyConfiguration, dbAdaptor, true, false));

//        studyConfiguration.getCohorts().put(cohortId, new HashSet<>(Arrays.asList(1, 2, 3, 4)));
//        studyConfiguration.getCohortIds().put(VariantSourceEntry.DEFAULT_COHORT, cohortId);
//        for (VariantWriter vw : writers) {
//            vw.includeStats(true);
//        }
//        taskList.add(new VariantStatsTask(reader, study1));

        VariantRunner vr = new VariantRunner(source1, reader, null, writers, taskList, 200);
        vr.run();

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
    public void testInsertMultiFiles() throws StorageManagerException {
        List<Variant> allVariants;
        studyConfiguration.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));

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
    public void testInsertMultiFilesMultiMerge() throws StorageManagerException {
        List<Variant> allVariants;
        studyConfiguration.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Float", "Integer"));
        studyConfiguration2.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("DP", "GQX"));
        studyConfiguration2.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS_TYPE.key(), Arrays.asList("Integer", "Float"));

        assertEquals(new MongoDBVariantWriteResult(3, 0, 0, 0, 0, 0), clearTime(loadFile1()));
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(3, allVariants.size());

        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();
        writeResult.merge(stageVariants(studyConfiguration2, createFile2Variants(), fileId2));
        writeResult.merge(stageVariants(studyConfiguration2, createFile3Variants(), fileId3));
        writeResult = mergeVariants(studyConfiguration2, Arrays.asList(fileId2, fileId3), writeResult);
        assertEquals(new MongoDBVariantWriteResult(2, 2, 0, 0, 0, 0), clearTime(writeResult));
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(5, allVariants.size());

        checkLoadedVariants(allVariants);

    }

    public void checkLoadedVariants(List<Variant> allVariants) {
        Variant variant;
        variant = allVariants.get(0);
        assertEquals(999, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName1), variant.getStudiesMap().keySet());

        variant = allVariants.get(1);
        assertEquals(1000, variant.getStart().longValue());
        assertEquals(new HashSet<>(Arrays.asList(studyName1, studyName2)), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration, fileId1, (sampleId) -> Integer.toString(sampleId + 10), "DP");

        variant = allVariants.get(2);
        assertEquals(1002, variant.getStart().longValue());
        assertEquals(new HashSet<>(Arrays.asList(studyName1, studyName2)), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration, fileId1, (sampleId) -> Integer.toString(sampleId + 10), "DP");
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> UNKNOWN_FIELD, "DP");
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyConfiguration2, fileId3, Object::toString, "DP");

        variant = allVariants.get(3);
        assertEquals(1004, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName2), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration2, fileId2, Object::toString, "DP");
        checkSampleData(variant, studyConfiguration2, fileId3, (sampleId) -> UNKNOWN_FIELD, "DP");
        checkSampleData(variant, studyConfiguration2, fileId3, (sampleId) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> sampleId % 2 == 0 ? UNKNOWN_FIELD : "0.7", "GQX");
        checkSampleData(variant, studyConfiguration2, fileId3, (sampleId) -> UNKNOWN_FIELD, "GQX");

        variant = allVariants.get(4);
        assertEquals(1006, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName2), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> UNKNOWN_FIELD, "DP");
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> UNKNOWN_FIELD, "GQX");
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyConfiguration2, fileId3, Object::toString, "DP");
        checkSampleData(variant, studyConfiguration2, fileId3, (sampleId) -> "0.7", "GQX");
    }


    public void checkSampleData(Variant variant, StudyConfiguration studyConfiguration, Integer fileId, Function<Integer, String>
            valueProvider, String field) {
        assertTrue(studyConfiguration.getFileIds().values().contains(fileId));
        studyConfiguration.getSamplesInFiles().get(fileId).forEach((sampleId) ->
                assertEquals("FileId=" + fileId + " Field=" + field + " Sample=" + sampleId, valueProvider.apply(sampleId),
                        variant.getStudy(studyConfiguration.getStudyName())
                        .getSampleData(studyConfiguration.getSampleIds().inverse().get(sampleId), field))
        );
    }

    public MongoDBVariantWriteResult loadFile1() throws StorageManagerException {
        return loadFile(studyConfiguration, createFile1Variants(source1), Integer.parseInt(source1.getFileId()));
    }

    public MongoDBVariantWriteResult loadFile2() throws StorageManagerException {
        return loadFile(studyConfiguration2, createFile2Variants(source2), Integer.parseInt(source2.getFileId()));
    }

    public MongoDBVariantWriteResult loadFile3() throws StorageManagerException {
        return loadFile(studyConfiguration2, createFile3Variants(source3), Integer.parseInt(source3.getFileId()));
    }

    public MongoDBVariantWriteResult loadFile(StudyConfiguration studyConfiguration, List<Variant> variants, int fileId)
            throws StorageManagerException {
//        return loadFileOld(studyConfiguration, variants, fileId);
        MongoDBVariantWriteResult stageWriteResult = stageVariants(studyConfiguration, variants, fileId);
        return mergeVariants(studyConfiguration, fileId, stageWriteResult);
    }

    public MongoDBVariantWriteResult loadFileOld(StudyConfiguration studyConfiguration, List<Variant> variants, int fileId)
            throws StorageManagerException {
        VariantMongoDBWriter mongoDBWriter;
        mongoDBWriter = new VariantMongoDBWriter(fileId, studyConfiguration, dbAdaptor, true, false);
        mongoDBWriter.setThreadSynchronizationBoolean(new AtomicBoolean(false));
        mongoDBWriter.open();
        mongoDBWriter.pre();

        variants.forEach(mongoDBWriter::write);

        mongoDBWriter.post();
        mongoDBWriter.close();
        studyConfiguration.getIndexedFiles().add(fileId);

        return mongoDBWriter.getWriteResult();
    }

    public MongoDBVariantWriteResult stageVariants(StudyConfiguration studyConfiguration, List<Variant> variants, int fileId) {
        MongoDBCollection stage = dbAdaptor.getDB().getCollection("stage");
        MongoDBVariantStageLoader variantStageLoader = new MongoDBVariantStageLoader(stage, studyConfiguration.getStudyId(), fileId, variants.size());

        variantStageLoader.insert(variants);

        return variantStageLoader.getWriteResult();
    }

    public MongoDBVariantWriteResult mergeVariants(StudyConfiguration studyConfiguration, int fileId,
                                                   MongoDBVariantWriteResult stageWriteResult) {

        return mergeVariants(studyConfiguration, Collections.singletonList(fileId), stageWriteResult);
    }
    public MongoDBVariantWriteResult mergeVariants(StudyConfiguration studyConfiguration, List<Integer> fileIds,
                                                   MongoDBVariantWriteResult stageWriteResult) {
        MongoDBCollection stage = dbAdaptor.getDB().getCollection("stage");
        MongoDBCollection variantsCollection = dbAdaptor.getDB().getCollection("variants");
        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(stage, studyConfiguration.getStudyId());
        MongoDBVariantMerger dbMerger = new MongoDBVariantMerger(dbAdaptor, studyConfiguration, fileIds,
                variantsCollection, reader.countAproxNumVariants(), studyConfiguration.getIndexedFiles());

        reader.open();
        reader.pre();

        List<Document> batch = reader.read(100);
        while (batch != null && !batch.isEmpty()) {
            dbMerger.apply(batch);
            batch = reader.read(100);
        }

        reader.post();
        reader.close();

        MongoDBVariantStageLoader.cleanStageCollection(stage, studyConfiguration.getStudyId(), fileIds);
        studyConfiguration.getIndexedFiles().addAll(fileIds);
        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
        return dbMerger.getResult().setSkippedVariants(stageWriteResult.getSkippedVariants());
    }

    public List<Variant> createFile1Variants() {
        return createFile1Variants(source1);
    }
    public List<Variant> createFile2Variants() {
        return createFile2Variants(source2);
    }
    public List<Variant> createFile3Variants() {
        return createFile3Variants(source3);
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile1Variants(VariantSource source1) {

        Variant variant;
        StudyEntry sourceEntry;
        List<Variant> variants = new LinkedList<>();
        variant = new Variant("X", 999, 999, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant("X", 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant("X", 1002, 1002, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "0/1").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "0/0").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "1/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "0/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        return variants;
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile2Variants(VariantSource source2) {
        Variant variant;
        StudyEntry sourceEntry;
        List<Variant> variants = new LinkedList<>();

        variant = new Variant("X", 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(source2.getFileId(), source2.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "1").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "2").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "3").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "4").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant("X", 1004, 1004, "A", "C");
        sourceEntry = new StudyEntry(source2.getFileId(), source2.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "0/1").append("DP", "1").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "0/0").append("DP", "2").append("GQX", ".")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "1/0").append("DP", "3").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "0/0").append("DP", "4").append("GQX", "..")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        return variants;
    }

    @SuppressWarnings("unchecked")
    public static List<Variant> createFile3Variants(VariantSource source3) {

        Variant variant;
        StudyEntry sourceEntry;
        List<Variant> variants = new LinkedList<>();

        variant = new Variant("X", 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(source3.getFileId(), source3.getStudyId());
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant("X", 1002, 1002, "A", "C");
        sourceEntry = new StudyEntry(source3.getFileId(), source3.getStudyId());
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        variant = new Variant("X", 1006, 1006, "A", "C");
        sourceEntry = new StudyEntry(source3.getFileId(), source3.getStudyId());
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        variants.add(variant);

        return variants;
    }

    @Test
    public void testInsertSameVariantTwice() throws StorageManagerException {

        VariantMongoDBWriter mongoDBWriter;
        StudyEntry sourceEntry;
        mongoDBWriter = new VariantMongoDBWriter(fileId1, studyConfiguration, dbAdaptor, true, false);
        mongoDBWriter.setThreadSynchronizationBoolean(new AtomicBoolean(false));
        mongoDBWriter.open();
        mongoDBWriter.pre();

        Variant variant1 = new Variant("X", 999, 999, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19660", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19661", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19685", (Collections.singletonMap("GT", "./.")));
        variant1.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant1);
        System.out.println("mongoDBWriter = " + mongoDBWriter.getWriteResult());
        assertEquals(new MongoDBVariantWriteResult(1, 0, 0, 0, 0, 0), clearTime(mongoDBWriter.getWriteResult()));

        Variant variant2 = new Variant("X", 999, 999, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19660", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19661", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19685", (Collections.singletonMap("GT", "1/1")));
        variant2.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant2);
        System.out.println("mongoDBWriter = " + mongoDBWriter.getWriteResult());
        assertEquals(new MongoDBVariantWriteResult(1, 0, 0, 0, 0, 1), clearTime(mongoDBWriter.getWriteResult()));

        Variant variant3 = new Variant("X", 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19660", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19661", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19685", (Collections.singletonMap("GT", "1/1")));
        variant3.addStudyEntry(sourceEntry);

        Variant variant4 = new Variant("X", 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19660", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19661", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19685", (Collections.singletonMap("GT", "./.")));
        variant4.addStudyEntry(sourceEntry);

        mongoDBWriter.write(Arrays.asList(variant3, variant4));
        System.out.println("mongoDBWriter = " + mongoDBWriter.getWriteResult());
        assertEquals(new MongoDBVariantWriteResult(2, 0, 0, 0, 0, 2), clearTime(mongoDBWriter.getWriteResult()));

        Variant variant5 = new Variant("X", 1000, 1000, "A", "<CN0>");
        variant5.setType(VariantType.SYMBOLIC);
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19660", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19661", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19685", (Collections.singletonMap("GT", "./.")));
        variant5.addStudyEntry(sourceEntry);
        mongoDBWriter.write(Collections.singletonList(variant5));
        assertEquals(new MongoDBVariantWriteResult(2, 0, 0, 0, 1, 2), clearTime(mongoDBWriter.getWriteResult()));

        mongoDBWriter.post();
        mongoDBWriter.close();
        studyConfiguration.getIndexedFiles().add(fileId1);
    }

    public MongoDBVariantWriteResult clearTime(MongoDBVariantWriteResult writeResult) {
        return writeResult.setExistingVariantsNanoTime(0).setFillGapsNanoTime(0).setNewVariantsNanoTime(0);
    }

}
