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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

/**
 * Created on 13/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantDBAdaptorPhasedTest extends VariantStorageBaseTest {


    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        ObjectMap options = new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.CALCULATE_STATS.key(), false)
                .append(VariantStorageOptions.EXTRA_GENOTYPE_FIELDS.key(), "DP,PS");
        runDefaultETL(getResourceUri("variant-test-phased.vcf"), variantStorageManager, newStudyMetadata(), options);

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();
        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant.toJson());
        }

    }

    @Test
    public void queryPhased() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        DataResult<Variant> result;

        result = dbAdaptor.getPhased("1:819411:A:G", STUDY_NAME, "SAMPLE_1", new QueryOptions(), 1000);
        Assert.assertEquals(4, result.getNumResults());
        Assert.assertEquals("1:819320:A:C", result.getResults().get(0).toString());
        Assert.assertEquals("1:819411:A:G", result.getResults().get(1).toString());
        Assert.assertEquals("1:819651:A:G", result.getResults().get(2).toString());
        Assert.assertEquals("1:820211:T:C", result.getResults().get(3).toString());

        result = dbAdaptor.getPhased("1:819411:A:G", STUDY_NAME, "SAMPLE_1", new QueryOptions(), 100000000);
        Assert.assertEquals(4, result.getNumResults());
        Assert.assertEquals("1:819320:A:C", result.getResults().get(0).toString());
        Assert.assertEquals("1:819411:A:G", result.getResults().get(1).toString());
        Assert.assertEquals("1:819651:A:G", result.getResults().get(2).toString());
        Assert.assertEquals("1:820211:T:C", result.getResults().get(3).toString());


        result = dbAdaptor.getPhased("1:819411:A:G", STUDY_NAME, "SAMPLE_2", new QueryOptions(), 100000000);
        Assert.assertEquals(4, result.getNumResults());
        Assert.assertEquals("1:819411:A:G", result.getResults().get(0).toString());
        Assert.assertEquals("1:819651:A:G", result.getResults().get(1).toString());
        Assert.assertEquals("1:820211:T:C", result.getResults().get(2).toString());
        Assert.assertEquals("1:820811:G:C", result.getResults().get(3).toString());

        result = dbAdaptor.getPhased("1:819320:A:C", STUDY_NAME, "SAMPLE_2", new QueryOptions(), 100000000);
        Assert.assertEquals(0, result.getNumResults());

        result = dbAdaptor.getPhased("1:734964:T:C", STUDY_NAME, "SAMPLE_2", new QueryOptions(), 100000000);
        Assert.assertEquals(0, result.getNumResults());

    }

}
