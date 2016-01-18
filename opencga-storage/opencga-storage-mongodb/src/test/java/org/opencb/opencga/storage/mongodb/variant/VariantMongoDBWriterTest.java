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
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.mongodb.variant.DBObjectToSamplesConverter.UNKNOWN_GENOTYPE;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
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
     * <p>
     * |---------|-------|---------------|
     * |         | Study1|    Study2     |
     * | Variant |-------|---------------|
     * |         | File1 | File2 | File3 |
     * |---------|-------|-------|-------|
     * | 999     |   x   |       |       |
     * | 1000    |   x   |   x   |   x   |
     * | 1002    |   x   |       |   x   |
     * | 1004    |       |   x   |       |
     * | 1006    |       |       |   x   |
     * |---------|-------|-------|-------|
     *
     * @throws StorageManagerException
     */
    @Test
    public void testInsertMultiFiles() throws StorageManagerException {
        List<Variant> allVariants;
        studyConfiguration.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GQX", "DP"));
        studyConfiguration2.getAttributes().put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("DP", "GQX"));

        assertEquals(new MongoDBVariantWriteResult(3, 0, 0, 0), loadFile1());
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(3, allVariants.size());

        assertEquals(new MongoDBVariantWriteResult(1, 1, 0, 0), loadFile2());
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(4, allVariants.size());

        assertEquals(new MongoDBVariantWriteResult(1, 2, 0, 0), loadFile3());
        allVariants = dbAdaptor.get(new Query(), new QueryOptions("sort", true)).getResult();
        assertEquals(5, allVariants.size());

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
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> DBObjectToSamplesConverter.UNKNOWN_FIELD.toString(), "DP");
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyConfiguration2, fileId3, Object::toString, "DP");

        variant = allVariants.get(3);
        assertEquals(1004, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName2), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration2, fileId2, Object::toString, "DP");
        checkSampleData(variant, studyConfiguration2, fileId3, (sampleId) -> DBObjectToSamplesConverter.UNKNOWN_FIELD.toString(), "DP");
        checkSampleData(variant, studyConfiguration2, fileId3, (sampleId) -> UNKNOWN_GENOTYPE, "GT");

        variant = allVariants.get(4);
        assertEquals(1006, variant.getStart().longValue());
        assertEquals(Collections.singleton(studyName2), variant.getStudiesMap().keySet());
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> DBObjectToSamplesConverter.UNKNOWN_FIELD.toString(), "DP");
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> DBObjectToSamplesConverter.UNKNOWN_FIELD.toString(), "GQX");
        checkSampleData(variant, studyConfiguration2, fileId2, (sampleId) -> UNKNOWN_GENOTYPE, "GT");
        checkSampleData(variant, studyConfiguration2, fileId3, Object::toString, "DP");
        checkSampleData(variant, studyConfiguration2, fileId3, (sampleId) -> "0.7", "GQX");

    }

    public void checkSampleData(Variant variant, StudyConfiguration studyConfiguration, Integer fileId, Function<Integer, String>
            valueProvider, String field) {
        assertTrue(studyConfiguration.getFileIds().values().contains(fileId));
        studyConfiguration.getSamplesInFiles().get(fileId).forEach((sampleId) ->
                assertEquals(valueProvider.apply(sampleId), variant.getStudy(studyConfiguration.getStudyName())
                        .getSampleData(studyConfiguration.getSampleIds().inverse().get(sampleId), field))
        );
    }

    @SuppressWarnings("unchecked")
    public MongoDBVariantWriteResult loadFile1() throws StorageManagerException {
        VariantMongoDBWriter mongoDBWriter;
        Variant variant;
        StudyEntry sourceEntry;
        mongoDBWriter = new VariantMongoDBWriter(fileId1, studyConfiguration, dbAdaptor, true, false);
        mongoDBWriter.setThreadSynchronizationBoolean(new AtomicBoolean(false));
        mongoDBWriter.open();
        mongoDBWriter.pre();

        variant = new Variant("X", 999, 999, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant);

        variant = new Variant("X", 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant);

        variant = new Variant("X", 1002, 1002, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "0/1").append("DP", "11").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "0/0").append("DP", "12").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "1/0").append("DP", "13").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "0/0").append("DP", "14").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant);

        mongoDBWriter.post();
        mongoDBWriter.close();
        studyConfiguration.getIndexedFiles().add(fileId1);

        return mongoDBWriter.getWriteResult();
    }

    @SuppressWarnings("unchecked")
    public MongoDBVariantWriteResult loadFile2() throws StorageManagerException {
        VariantMongoDBWriter mongoDBWriter;
        Variant variant;
        StudyEntry sourceEntry;
        mongoDBWriter = new VariantMongoDBWriter(fileId2, studyConfiguration2, dbAdaptor, true, false);
        mongoDBWriter.setThreadSynchronizationBoolean(new AtomicBoolean(false));
        mongoDBWriter.open();
        mongoDBWriter.pre();

        variant = new Variant("X", 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(source2.getFileId(), source2.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "./.").append("DP", "1").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "1/1").append("DP", "2").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "0/0").append("DP", "3").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "1/0").append("DP", "4").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant);

        variant = new Variant("X", 1004, 1004, "A", "C");
        sourceEntry = new StudyEntry(source2.getFileId(), source2.getStudyId());
        sourceEntry.addSampleData("NA19600", ((Map) new ObjectMap("GT", "0/1").append("DP", "1").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19660", ((Map) new ObjectMap("GT", "0/0").append("DP", "2").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19661", ((Map) new ObjectMap("GT", "1/0").append("DP", "3").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA19685", ((Map) new ObjectMap("GT", "0/0").append("DP", "4").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant);

        mongoDBWriter.post();
        mongoDBWriter.close();
        studyConfiguration2.getIndexedFiles().add(fileId2);

        return mongoDBWriter.getWriteResult();
    }

    @SuppressWarnings("unchecked")
    public MongoDBVariantWriteResult loadFile3() throws StorageManagerException {
        VariantMongoDBWriter mongoDBWriter;
        Variant variant;
        StudyEntry sourceEntry;
        mongoDBWriter = new VariantMongoDBWriter(fileId3, studyConfiguration2, dbAdaptor, true, false);
        mongoDBWriter.setThreadSynchronizationBoolean(new AtomicBoolean(false));
        mongoDBWriter.open();
        mongoDBWriter.pre();


        variant = new Variant("X", 1000, 1000, "A", "C");
        sourceEntry = new StudyEntry(source3.getFileId(), source3.getStudyId());
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant);

        variant = new Variant("X", 1002, 1002, "A", "C");
        sourceEntry = new StudyEntry(source3.getFileId(), source3.getStudyId());
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant);

        variant = new Variant("X", 1006, 1006, "A", "C");
        sourceEntry = new StudyEntry(source3.getFileId(), source3.getStudyId());
        sourceEntry.addSampleData("NA00001.X", ((Map) new ObjectMap("GT", "0/1").append("DP", "5").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00002.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "6").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00003.X", ((Map) new ObjectMap("GT", "1/0").append("DP", "7").append("GQX", "0.7")));
        sourceEntry.addSampleData("NA00004.X", ((Map) new ObjectMap("GT", "0/0").append("DP", "8").append("GQX", "0.7")));
        variant.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant);

        mongoDBWriter.post();
        mongoDBWriter.close();
        studyConfiguration2.getIndexedFiles().add(fileId3);

        return mongoDBWriter.getWriteResult();
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
        assertEquals(new MongoDBVariantWriteResult(1, 0, 0, 0), mongoDBWriter.getWriteResult());

        Variant variant2 = new Variant("X", 999, 999, "A", "C");
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19660", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19661", (Collections.singletonMap("GT", "1/1")));
        sourceEntry.addSampleData("NA19685", (Collections.singletonMap("GT", "1/1")));
        variant2.addStudyEntry(sourceEntry);
        mongoDBWriter.write(variant2);
        System.out.println("mongoDBWriter = " + mongoDBWriter.getWriteResult());
        assertEquals(new MongoDBVariantWriteResult(1, 0, 0, 1), mongoDBWriter.getWriteResult());

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
        assertEquals(new MongoDBVariantWriteResult(2, 0, 0, 2), mongoDBWriter.getWriteResult());

        Variant variant5 = new Variant("X", 1000, 1000, "A", "<CN0>");
        variant5.setType(VariantType.SYMBOLIC);
        sourceEntry = new StudyEntry(source1.getFileId(), source1.getStudyId());
        sourceEntry.addSampleData("NA19600", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19660", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19661", (Collections.singletonMap("GT", "./.")));
        sourceEntry.addSampleData("NA19685", (Collections.singletonMap("GT", "./.")));
        variant5.addStudyEntry(sourceEntry);
        mongoDBWriter.write(Collections.singletonList(variant5));
        assertEquals(new MongoDBVariantWriteResult(2, 0, 1, 2), mongoDBWriter.getWriteResult());

        mongoDBWriter.post();
        mongoDBWriter.close();
        studyConfiguration.getIndexedFiles().add(fileId1);
    }

}
