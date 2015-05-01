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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
@Ignore
public abstract class VariantDBAdaptorTest extends VariantStorageManagerTest {

    private static ETLResult etlResult;
    private VariantDBAdaptor dbAdaptor;

    @BeforeClass
    public static void beforeClass() {
        etlResult = null;
    }

    @Before
    public void before() throws Exception {
        if (etlResult == null) {
            StudyConfiguration studyConfiguration = newStudyConfiguration();
            clearDB();
            etlResult = runDefaultETL(getVariantStorageManager(), studyConfiguration);
        }
        dbAdaptor = getVariantStorageManager().getDBAdaptor(null, null);
    }

    @After
    public void after() {
        dbAdaptor.close();
    }





}
