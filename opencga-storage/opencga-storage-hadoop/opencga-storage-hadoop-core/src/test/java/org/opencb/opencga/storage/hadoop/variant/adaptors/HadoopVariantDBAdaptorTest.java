/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.*;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import static org.junit.Assert.assertTrue;


/**
 * Created on 20/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantDBAdaptorTest extends VariantDBAdaptorTest implements HadoopVariantStorageTest {


    @Before
    @Override
    public void before() throws Exception {
        boolean fileIndexed = VariantDBAdaptorTest.fileIndexed;
        try {
            super.before();
        } finally {
            try {
                if (!fileIndexed) {
                    VariantHbaseTestUtils.printVariantsFromVariantsTable((VariantHadoopDBAdaptor) dbAdaptor);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

//    @Override
//    protected ObjectMap getOtherParams() {
//        return new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "proto")
//                .append(HadoopVariantStorageEngine.HADOOP_LOAD_DIRECT, true)
//                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);
//    }
    @Override
    protected ObjectMap getOtherParams() {
        return new ObjectMap(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro")
                .append(HadoopVariantStorageEngine.HADOOP_LOAD_DIRECT, false)
                .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "")
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);
    }


    @Override
    @Ignore
    public void rank_gene() throws Exception {
        Assume.assumeTrue(false);
        super.rank_gene();
    }

    @Override
    @Ignore
    public void testExcludeFiles() {
        Assume.assumeTrue(false);
        super.testExcludeFiles();
    }

    @Override
    @Ignore
    public void testGetAllVariants_missingAllele() throws Exception {
        Assume.assumeTrue(false);
        super.testGetAllVariants_missingAllele();
    }

    @Override
    @Ignore
    public void groupBy_gene_limit_0() throws Exception {
        Assume.assumeTrue(false);
        super.groupBy_gene_limit_0();
    }

    @Override
    @Ignore
    public void groupBy_gene() throws Exception {
        Assume.assumeTrue(false);
        super.groupBy_gene();
    }

    @Override
    @Ignore
    public void testGetAllVariants_files() {
        Assume.assumeTrue(false);
        super.testGetAllVariants_files();
    }

    @Override
    @Ignore
    public void rank_ct() throws Exception {
        Assume.assumeTrue(false);
        super.rank_ct();
    }

    @Override
    @Ignore
    public void testGetAllVariants() {
        Assume.assumeTrue(false);
        super.testGetAllVariants();
    }

    @Override
    @Ignore
    public void testInclude() {
        Assume.assumeTrue(false);
        super.testInclude();
    }

    @Override
    @Ignore
    public void testGetAllVariants_genotypes() {
        Assume.assumeTrue(false);
        super.testGetAllVariants_genotypes();
    }

    @Override
    @Ignore
    public void testGetAllVariants_cohorts() throws Exception {
        Assume.assumeTrue(false);
        super.testGetAllVariants_cohorts();
    }

    @Override
    @Ignore
    public void testIterator() {
        Assume.assumeTrue(false);
        super.testIterator();
    }
}
