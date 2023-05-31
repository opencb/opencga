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

package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED;


/**
 * Created on 20/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@RunWith(Parameterized.class)
@Category(LongTests.class)
public class HadoopVariantDBAdaptorTest extends VariantDBAdaptorTest implements HadoopVariantStorageTest {

    private static final boolean GROUP_BY = false;
    protected static final boolean MISSING_ALLELE = false;

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Parameter
    public ObjectMap indexParams;

    public static ObjectMap previousIndexParams = null;
    protected CellBaseUtils cellBaseUtils;
    private long expectedConnections;

    @Parameters
    public static List<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        parameters.add(new Object[]{
                new ObjectMap()
                        .append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
//                        .append(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DS,GL")
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), true)
                        .append(VariantStorageOptions.GVCF.key(), false)
        });
//        parameters.add(new Object[]{
//                new ObjectMap()
//                        .append(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "proto")
//                        .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.ADVANCED)
////                        .append(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DS,GL")
//                        .append(VariantStorageEngine.Options.STATS_CALCULATE.key(), true)
//                        .append(VariantStorageEngine.Options.GVCF.key(), false)
//        });
        return parameters;
    }

    @Before
    @Override
    public void before() throws Exception {
        boolean fileIndexed = VariantDBAdaptorTest.fileIndexed;
        try {
            VariantStorageEngine.MergeMode mergeMode = VariantStorageEngine.MergeMode.from(indexParams);
            if (!indexParams.equals(previousIndexParams)) {
                fileIndexed = false;
                VariantDBAdaptorTest.fileIndexed = false;
                clearDB(getVariantStorageEngine().getDBName());
            }
            previousIndexParams = indexParams;
            System.out.println("Loading with MergeMode : " + mergeMode);
            super.before();
        } finally {
            try {
                if (!fileIndexed) {
                    VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
                    studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
                    studyMetadata.getAttributes().put(MISSING_GENOTYPES_UPDATED, true);
                    dbAdaptor.getMetadataManager().unsecureUpdateStudyMetadata(studyMetadata);
                    VariantHbaseTestUtils.printVariants(studyMetadata, dbAdaptor, newOutputUri());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        cellBaseUtils = variantStorageEngine.getCellBaseUtils();
        expectedConnections = GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getTotalSum();
    }

    @After
    public void after() throws IOException {
        assertEquals(expectedConnections, GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getTotalSum());
        super.after();
    }

    //
    //    @Override
//    public Map<String, ?> getOtherStorageConfigurationOptions() {
//        return new ObjectMap(AbstractHadoopVariantStoragePipeline.SKIP_CREATE_PHOENIX_INDEXES, true);
//    }

//    @Override
//    protected String getHetGT() {
//        return Genotype.HET_REF;
//    }
//
    @Override
    protected String getHomRefGT() {
        return Genotype.HOM_REF;
    }
//
//    @Override
//    protected String getHomAltGT() {
//        return Genotype.HOM_VAR;
//    }

    @Override
    protected ObjectMap getOtherParams() {
        return indexParams;
    }


    @Override
    public void rank_gene() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.rank_gene();
    }

    @Override
    public void testGetAllVariants_missingAllele() throws Exception {
        Assume.assumeTrue(MISSING_ALLELE);
        super.testGetAllVariants_missingAllele();
    }

    @Override
    @Ignore
    public void groupBy_gene_limit_0() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.groupBy_gene_limit_0();
    }

    @Override
    public void groupBy_gene() throws Exception {
        Assume.assumeTrue(GROUP_BY);
        super.groupBy_gene();
    }

    @Override
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
    public void testCombineBtSoFlag() {
        Assume.assumeTrue("HBase returns more elements than expected", false);
        super.testCombineBtSoFlag();
    }

    @Test
    public void testNativeQuery() {
        int count = 0;
        for (VariantDBIterator iterator = dbAdaptor.iterator(new Query(), new QueryOptions(VariantHadoopDBAdaptor.NATIVE, true)); iterator.hasNext();) {
            Variant variant = iterator.next();
//            System.out.println(variant.toJson());
            count++;
        }
        assertEquals(dbAdaptor.count(new Query()).first().intValue(), count);
    }

    @Test
    public void testArchiveIterator() {
        int count = 0;
        Query query = new Query(VariantQueryParam.STUDY.key(), studyMetadata.getId())
                .append(VariantQueryParam.FILE.key(), UriUtils.fileName(smallInputUri));

        VariantDBIterator iterator = dbAdaptor.iterator(query, new QueryOptions("archive", true));
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
//            System.out.println(variant.toJson());
            count++;
        }
        assertEquals(fileMetadata.getStats().getVariantCount().intValue(), count);
    }

}
