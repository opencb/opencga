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

import org.bson.Document;
import org.hamcrest.core.IsInstanceOf;
import org.hamcrest.core.StringContains;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;


/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class MongoVariantStorageManagerTest extends VariantStorageManagerTest implements MongoVariantStorageManagerTestUtils {

//    @Rule
//    public ExpectedException thrown = ExpectedException.none();
//
//    @Override
//    protected ExpectedException getThrown() {
//        return thrown;
//    }


    @Test
    public void stageResumeFromErrorTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        BatchFileOperation operation = new BatchFileOperation(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(),
                Collections.singletonList(FILE_ID), System.currentTimeMillis());
        operation.addStatus(new Date(System.currentTimeMillis() - 100), BatchFileOperation.Status.RUNNING);
        operation.addStatus(new Date(System.currentTimeMillis() - 50), BatchFileOperation.Status.ERROR);
        // Last status is ERROR

        studyConfiguration.getBatches().add(operation);
        MongoDBVariantStorageManager variantStorageManager = getVariantStorageManager();
        variantStorageManager.getDBAdaptor(DB_NAME).getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);

        System.out.println("----------------");
        System.out.println("|   RESUME     |");
        System.out.println("----------------");

        runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE_RESUME.key(), false)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
        );

    }

    @Test
    public void stageForceResumeTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        BatchFileOperation operation = new BatchFileOperation(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(),
                Collections.singletonList(FILE_ID), System.currentTimeMillis());
        operation.addStatus(new Date(System.currentTimeMillis() - 100), BatchFileOperation.Status.RUNNING);
        operation.addStatus(new Date(System.currentTimeMillis() - 50), BatchFileOperation.Status.ERROR);
        operation.addStatus(new Date(System.currentTimeMillis()), BatchFileOperation.Status.RUNNING);
        // Last status is RUNNING
        studyConfiguration.getBatches().add(operation);
        MongoDBVariantStorageManager variantStorageManager = getVariantStorageManager();
        variantStorageManager.getDBAdaptor(DB_NAME).getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);

        try {
            runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration);
            fail();
        } catch (StorageManagerException e) {
            e.printStackTrace();
            assertTrue(e.getCause().getMessage().contains("is being loaded in the stage collection right now"));
        }

        System.out.println("----------------");
        System.out.println("|   RESUME     |");
        System.out.println("----------------");

        runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE_RESUME.key(), true)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
        );
    }

    @Test
    public void mergeAlreadyStagedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.MERGE.key(), false));

        runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.MERGE.key(), true), false, false, true);

        Long count = variantStorageManager.getDBAdaptor(DB_NAME).count(null).first();
        assertTrue(count > 0);
    }


    @Test
    public void stageAlreadyStagedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.MERGE.key(), false));

        long count = variantStorageManager.getDBAdaptor(DB_NAME).count(null).first();
        assertEquals(0L, count);

//        thrown.expect(StorageETLException.class);
//        thrown.expectCause(IsInstanceOf.instanceOf(StorageManagerException.class));
//        thrown.expectCause(
//                ThrowableMessageMatcher.hasMessage(
//                        StringContains.containsString(
//                                StorageManagerException.alreadyLoaded(FILE_ID, studyConfiguration).getMessage())));
        runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.MERGE.key(), false), false, false, true);
    }


    @Test
    public void stageAlreadyMergedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.MERGE.key(), true));

        Long count = variantStorageManager.getDBAdaptor(DB_NAME).count(null).first();
        assertTrue(count > 0);

        thrown.expect(StorageETLException.class);
        thrown.expectCause(IsInstanceOf.instanceOf(StorageManagerException.class));
        thrown.expectCause(
                ThrowableMessageMatcher.hasMessage(
                        StringContains.containsString(
                                StorageManagerException.alreadyLoaded(FILE_ID, studyConfiguration).getMessage())));
        runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.MERGE.key(), false), false, false, true);

    }

    @Test
    public void mergeAlreadyMergedFileTest() throws Exception {
        StudyConfiguration studyConfiguration = createStudyConfiguration();

        StorageETLResult storageETLResult = runDefaultETL(smallInputUri, variantStorageManager, studyConfiguration, new ObjectMap()
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.MERGE.key(), true));

        Long count = variantStorageManager.getDBAdaptor(DB_NAME).count(null).first();
        assertTrue(count > 0);

        thrown.expect(StorageETLException.class);
        thrown.expectCause(IsInstanceOf.instanceOf(StorageManagerException.class));
        thrown.expectCause(
                ThrowableMessageMatcher.hasMessage(
                        StringContains.containsString(
                                StorageManagerException.alreadyLoaded(FILE_ID, studyConfiguration).getMessage())));
        runETL(variantStorageManager, storageETLResult.getTransformResult(), outputUri, new ObjectMap()
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.STAGE.key(), true)
                .append(MongoDBVariantStorageManager.MongoDBVariantOptions.MERGE.key(), true), false, false, true);

    }

    @Test
    public void checkCanLoadSampleBatchTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 1);
        studyConfiguration.getIndexedFiles().add(1);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 2);
        studyConfiguration.getIndexedFiles().add(2);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 3);
        studyConfiguration.getIndexedFiles().add(3);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 4);
        studyConfiguration.getIndexedFiles().add(4);
    }

    @Test
    public void checkCanLoadSampleBatch2Test() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 4);
        studyConfiguration.getIndexedFiles().add(4);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 3);
        studyConfiguration.getIndexedFiles().add(3);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 2);
        studyConfiguration.getIndexedFiles().add(2);
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 1);
        studyConfiguration.getIndexedFiles().add(1);
    }

    @Test
    public void checkCanLoadSampleBatchFailTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        studyConfiguration.getIndexedFiles().addAll(Arrays.asList(1, 3, 4));
        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("Another sample batch has been loaded");
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 2);
    }

    @Test
    public void checkCanLoadSampleBatchFail2Test() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        studyConfiguration.getIndexedFiles().addAll(Arrays.asList(1, 2));
        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("There was some already indexed samples, but not all of them");
        MongoDBVariantStorageETL.checkCanLoadSampleBatch(studyConfiguration, 5);
    }

    @SuppressWarnings("unchecked")
    public StudyConfiguration createStudyConfiguration() {
        StudyConfiguration studyConfiguration = new StudyConfiguration(5, "study");
        LinkedHashSet<Integer> batch1 = new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4));
        LinkedHashSet<Integer> batch2 = new LinkedHashSet<>(Arrays.asList(5, 6, 7, 8));
        LinkedHashSet<Integer> batch3 = new LinkedHashSet<>(Arrays.asList(1, 3, 5, 7)); //Mixed batch
        studyConfiguration.getSamplesInFiles().put(1, batch1);
        studyConfiguration.getSamplesInFiles().put(2, batch1);
        studyConfiguration.getSamplesInFiles().put(3, batch2);
        studyConfiguration.getSamplesInFiles().put(4, batch2);
        studyConfiguration.getSamplesInFiles().put(5, batch3);
        studyConfiguration.getSampleIds().putAll(((Map) new ObjectMap()
                .append("s1", 1)
                .append("s2", 2)
                .append("s3", 3)
                .append("s4", 4)
                .append("s5", 5)
                .append("s6", 6)
                .append("s7", 7)
                .append("s8", 8)
        ));
        return studyConfiguration;
    }

    @Test
    @Override
    public void multiIndexPlatinum() throws Exception {
        super.multiIndexPlatinum();

        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME)) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            for (Document document : variantsCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                String id = document.getString("_id");
                List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);
//                List alternates = studies.get(0).get(ALTERNATES_FIELD, List.class);
                if (id.equals("M_16185_C_A") || id.equals("M_16184_C_") || id.equals("M_16184_CC_")) {
                    continue;
                }
                assertEquals(id, 2, studies.size());
                for (Document study : studies) {
                    Document gts = study.get(GENOTYPES_FIELD, Document.class);
                    Set<Integer> samples = new HashSet<>();

                    for (Map.Entry<String, Object> entry : gts.entrySet()) {
                        List<Integer> sampleIds = (List<Integer>) entry.getValue();
                        for (Integer sampleId : sampleIds) {
                            assertFalse(id, samples.contains(sampleId));
                            assertTrue(id, samples.add(sampleId));
                        }
                    }
                    assertEquals("\"" + id + "\" study: " + study.get(STUDYID_FIELD), 17, samples.size());
                }

                Document gt1 = studies.get(0).get(GENOTYPES_FIELD, Document.class);
                Document gt2 = studies.get(1).get(GENOTYPES_FIELD, Document.class);
                assertEquals(id, gt1.keySet(), gt2.keySet());
                for (String gt : gt1.keySet()) {
                    // Order is not important. Compare using a set
                    assertEquals(id + ":" + gt, new HashSet<>(gt1.get(gt, List.class)), new HashSet<>(gt2.get(gt, List.class)));
                }

                //Order is very important!
                assertEquals(id, studies.get(0).get(ALTERNATES_FIELD), studies.get(1).get(ALTERNATES_FIELD));

                //Order is not important.
                Map<String, Document> files1 = ((List<Document>) studies.get(0).get(FILES_FIELD))
                        .stream()
                        .collect(Collectors.toMap(d -> d.get(FILEID_FIELD).toString(), Function.identity()));
                Map<String, Document> files2 = ((List<Document>) studies.get(1).get(FILES_FIELD))
                        .stream()
                        .collect(Collectors.toMap(d -> d.get(FILEID_FIELD).toString(), Function.identity()));
                assertEquals(id, studies.get(0).get(FILES_FIELD, List.class).size(), studies.get(1).get(FILES_FIELD, List.class).size());
                assertEquals(id, files1.size(), files2.size());
                for (Map.Entry<String, Document> entry : files1.entrySet()) {
                    assertEquals(id, entry.getValue(), files2.get(entry.getKey()));
                }

            }
        }
    }



    @Test
    @Override
    public void multiRegionBatchIndex() throws Exception {
        super.multiRegionBatchIndex();
        checkLoadedVariants();
    }

    @Test
    @Override
    public void multiRegionIndex() throws Exception {
        super.multiRegionIndex();

        checkLoadedVariants();
    }

    public void checkLoadedVariants() throws Exception {
        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME)) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            for (Document document : variantsCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                String id = document.getString("_id");
                List<Document> studies = document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class);

//                assertEquals(id, 2, studies.size());
                for (Document study : studies) {
                    Document gts = study.get(GENOTYPES_FIELD, Document.class);
                    Set<Integer> samples = new HashSet<>();

                    for (Map.Entry<String, Object> entry : gts.entrySet()) {
                        List<Integer> sampleIds = (List<Integer>) entry.getValue();
                        for (Integer sampleId : sampleIds) {
                            String message = "var: " + id + " Duplicated sampleId " + sampleId + " in gt " + entry.getKey() + " : " + sampleIds;
                            assertFalse(message, samples.contains(sampleId));
                            assertTrue(message, samples.add(sampleId));
                        }
                    }
                }
            }
        }
    }

    @Test
    @Override
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        super.indexWithOtherFieldsExcludeGT();

        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME)) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            for (Document document : variantsCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                assertFalse(((Document) document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class).get(0))
                        .containsKey(GENOTYPES_FIELD));
                System.out.println("dbObject = " + document);
            }
        }

    }
}
