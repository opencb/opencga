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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageSearchIntersectTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import static org.junit.Assert.assertFalse;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.INCLUDE_STUDY;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;

/**
 * Created on 04/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoVariantStorageSearchIntersectTest extends VariantStorageSearchIntersectTest implements MongoDBVariantStorageTest {

    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }


    @Test
    public void testDoQuerySearchManagerMongoSpecialRules() throws Exception {
        MongoDBVariantStorageEngine engine = getVariantStorageEngine();

        // SPECIAL CASE FOR MONGO
        assertFalse(engine.doQuerySearchManager(new Query(REGION.key(), "3:44-55"), new QueryOptions(VariantField.SUMMARY, true)));
        assertFalse(engine.doQuerySearchManager(new Query(REGION.key(), "3:44-55").append(STUDY.key(), 3), new QueryOptions(VariantField.SUMMARY, true)));
        assertFalse(engine.doQuerySearchManager(new Query(REGION.key(), "3:44-55").append(STUDY.key(), 3).append(INCLUDE_STUDY.key(), 3), new QueryOptions(VariantField.SUMMARY, true)));
    }
}
