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

package org.opencb.opencga.storage.core.variant;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.io.VariantReaderUtils.MALFORMED_FILE;

/**
 * Created on 01/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantStorageManagerTransformTest extends VariantStorageBaseTest {

    @Test
    public void transformIsolated() throws Exception {

        ObjectMap params = new ObjectMap();
        URI outputUri = newOutputUri();

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getDatabase()
                .setHosts(Collections.singletonList("1.1.1.1"));
        StoragePipelineResult etlResult = runETL(variantStorageManager, smallInputUri, outputUri, params, true, true, false);
        System.out.println("etlResult = " + etlResult);


        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(0, malformedFiles.length);

    }

    /**
     * Corrupted file index. This test must fail
     */
    @Test
    public void corruptedTransformNoFailTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), true);

        URI outputUri = newOutputUri();

        thrown.expect(StoragePipelineException.class);
        try {
            runETL(getVariantStorageEngine(), corruptedInputUri, outputUri, params, true, true, false);
        } catch (StoragePipelineException e) {
            assertEquals(1, e.getResults().size());

            System.out.println(e.getResults().get(0));
            assertTrue(e.getResults().get(0).isTransformExecuted());
            assertNotNull(e.getResults().get(0).getTransformError());
            assertTrue(e.getResults().get(0).getTransformTimeMillis() > 0);
            assertFalse(e.getResults().get(0).isLoadExecuted());

            String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
            assertEquals(1, malformedFiles.length);
            throw e;
        } catch (Exception e) {
            System.out.println("e.getClass().getName() = " + e.getClass().getName());
            throw e;
        }

    }

    @Test
    public void corruptedTransformTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), false);
        URI outputUri = newOutputUri();
        StoragePipelineResult result = runETL(getVariantStorageEngine(), corruptedInputUri, outputUri, params, true, true, false);

        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(1, malformedFiles.length);
        assertEquals(2, result.getTransformStats().getInt("malformed lines"));
    }


}
