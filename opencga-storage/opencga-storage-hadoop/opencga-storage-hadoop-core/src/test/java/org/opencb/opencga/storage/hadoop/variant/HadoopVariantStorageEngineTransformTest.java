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

package org.opencb.opencga.storage.hadoop.variant;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTransformTest;

import java.net.URI;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.io.VariantReaderUtils.MALFORMED_FILE;

/**
 * Created on 01/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageEngineTransformTest extends VariantStorageManagerTransformTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();


    @Test
    public void protoTransformTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "proto")
                .append("transform.proto.parallel", true);
        URI outputUri = newOutputUri();

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        StoragePipelineResult etlResult = runETL(variantStorageManager, smallInputUri, outputUri, params, true, true, false);
        System.out.println("etlResult = " + etlResult);


        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(0, malformedFiles.length);
    }

}
