package org.opencb.opencga.storage.hadoop.variant.query.executors;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutorTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;


@Category(LongTests.class)
public class HadoopVariantQueryExecutorTest extends VariantQueryExecutorTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Override
    public void initSolr() throws Exception {
        if (HadoopVariantStorageTest.HadoopSolrSupport.isSolrTestingAvailable()) {
            super.initSolr();
        } else {
            System.out.println("Solr testing not available");
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        boolean firstRun = !super.fileIndexed;
        super.setUp();
        if (firstRun) {
            VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        }
    }
}
