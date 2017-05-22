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

import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;


/**
 * Created on 20/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantDBAdaptorTest extends VariantDBAdaptorTest implements HadoopVariantStorageTest {


    private static final boolean FILES = false;
    private static final boolean GROUP_BY = false;
    private static final boolean CT_GENES = false;
    protected static final boolean MISSING_ALLELE = false;

    @Before
    @Override
    public void before() throws Exception {
        boolean fileIndexed = VariantDBAdaptorTest.fileIndexed;
        try {
            super.before();
        } finally {
            try {
                if (!fileIndexed) {
                    VariantHbaseTestUtils.printVariants(studyConfiguration, (VariantHadoopDBAdaptor) dbAdaptor, newOutputUri());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

//    @Override
//    public Map<String, ?> getOtherStorageConfigurationOptions() {
//        return new ObjectMap(AbstractHadoopVariantStoragePipeline.SKIP_CREATE_PHOENIX_INDEXES, true);
//    }

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Override
    protected String getHetGT() {
        return Genotype.HET_REF;
    }

    @Override
    protected String getHomRefGT() {
        return Genotype.HOM_REF;
    }

    @Override
    protected String getHomAltGT() {
        return Genotype.HOM_VAR;
    }

    @Override
    protected ObjectMap getOtherParams() {
        return new ObjectMap()
                .append(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "proto")
                .append(HadoopVariantStorageEngine.HADOOP_LOAD_DIRECT, true)
//                .append(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro")
//                .append(HadoopVariantStorageEngine.HADOOP_LOAD_DIRECT, false)
                .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "")
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);
    }


    @Override
    @Ignore
    public void rank_gene() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.rank_gene();
    }

    @Override
    @Ignore
    public void testExcludeFiles() {
        Assume.assumeTrue(FILES);
        super.testExcludeFiles();
    }

    @Override
    public void testReturnNoneFiles() {
        Assume.assumeTrue(FILES);
        super.testReturnNoneFiles();
    }

    @Override
    @Ignore
    public void testGetAllVariants_missingAllele() throws Exception {
        Assume.assumeTrue(MISSING_ALLELE);
        super.testGetAllVariants_missingAllele();
    }

    @Override
    public void testGetAllVariants_negatedGenotypesMixed() {
        thrown.expect(VariantQueryException.class);
        super.testGetAllVariants_negatedGenotypesMixed();
    }

    @Override
    @Ignore
    public void groupBy_gene_limit_0() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.groupBy_gene_limit_0();
    }

    @Override
    @Ignore
    public void groupBy_gene() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.groupBy_gene();
    }

    @Override
    @Ignore
    public void testGetAllVariants_files() {
        Assume.assumeTrue(FILES);
        super.testGetAllVariants_files();
    }

    @Override
    public void testGetAllVariants_filter() {
        Assume.assumeTrue(FILES);
        super.testGetAllVariants_filter();
    }

    @Override
    @Ignore
    public void rank_ct() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.rank_ct();
    }

    @Override
    public void limitSkip(Query query, QueryOptions options) {
        Assume.assumeTrue("Unable to paginate queries without sorting", options.getBoolean(QueryOptions.SORT, false));
        super.limitSkip(query, options);
    }

    @Override
    public void testGetAllVariants_ct_gene() {
        Assume.assumeTrue(CT_GENES);
        super.testGetAllVariants_ct_gene();
    }

    @Override
    @Ignore
    public void testInclude() {
        Assume.assumeTrue(FILES);
        super.testInclude();
    }

}
