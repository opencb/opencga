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

package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.junit.After;
import org.junit.Before;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorLargeTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBAdaptorLargeTest extends VariantDBAdaptorLargeTest implements MongoDBVariantStorageTest {

    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }

    @Override
    protected int skippedVariants() {
        return 4;
    }

    @Override
    public ObjectMap getExtraOptions() {
        return new ObjectMap(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
    }
}
