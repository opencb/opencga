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

import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;


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
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 1);
        studyConfiguration.getIndexedFiles().add(1);
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 2);
        studyConfiguration.getIndexedFiles().add(2);
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 3);
        studyConfiguration.getIndexedFiles().add(3);
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 4);
        studyConfiguration.getIndexedFiles().add(4);
    }

    @Test
    public void checkCanLoadSampleBatch2Test() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 4);
        studyConfiguration.getIndexedFiles().add(4);
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 3);
        studyConfiguration.getIndexedFiles().add(3);
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 2);
        studyConfiguration.getIndexedFiles().add(2);
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 1);
        studyConfiguration.getIndexedFiles().add(1);
    }

    @Test
    public void checkCanLoadSampleBatchFailTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        studyConfiguration.getIndexedFiles().addAll(Arrays.asList(1, 3, 4));
        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("Another sample batch has been loaded");
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 2);
    }

    @Test
    public void checkCanLoadSampleBatchFail2Test() throws StorageManagerException {
        StudyConfiguration studyConfiguration = createStudyConfiguration();
        studyConfiguration.getIndexedFiles().addAll(Arrays.asList(1, 2));
        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("There was some already indexed samples, but not all of them");
        MongoDBVariantStorageManager.checkCanLoadSampleBatch(studyConfiguration, 5);
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

}
