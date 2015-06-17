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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorTest;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBAdaptorTest extends VariantDBAdaptorTest {

    @Override
    protected MongoDBVariantStorageManager getVariantStorageManager() throws Exception {
        return MongoVariantStorageManagerTestUtils.getVariantStorageManager();
    }

    @Override
    protected void clearDB(String dbName) throws Exception {
        MongoVariantStorageManagerTestUtils.clearDB(dbName);
    }

    @Test
    public void deleteStudyTest() throws Exception {
        VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
        dbAdaptor.deleteStudy(studyConfiguration.getStudyId(), new QueryOptions("purge", false));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, VariantSourceEntry> entry : variant.getSourceEntries().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyConfiguration.getStudyId() + ""));
            }
        }
        QueryResult<Variant> allVariants = dbAdaptor.getAllVariants(new QueryOptions("limit", 1));
        assertEquals(NUM_VARIANTS, allVariants.getNumTotalResults());
        etlResult = null;
    }

    @Test
    public void deleteAndPurgeStudyTest() throws Exception {
        VariantMongoDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
        dbAdaptor.deleteStudy(studyConfiguration.getStudyId(), new QueryOptions("purge", true));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, VariantSourceEntry> entry : variant.getSourceEntries().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyConfiguration.getStudyId() + ""));
            }
        }
        QueryResult<Variant> allVariants = dbAdaptor.getAllVariants(new QueryOptions("limit", 1));
        assertEquals(0, allVariants.getNumTotalResults());
        etlResult = null;
    }

}