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

import com.mongodb.BasicDBObject;
import org.bson.Document;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;


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

    @Override
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        super.indexWithOtherFieldsExcludeGT();

        VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
        MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

        for (Document document : variantsCollection.nativeQuery().find(new BasicDBObject(), new QueryOptions())) {
            assertFalse(((Document) document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class).get(0))
                    .containsKey(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD));
            System.out.println("dbObject = " + document);
        }

    }
}
