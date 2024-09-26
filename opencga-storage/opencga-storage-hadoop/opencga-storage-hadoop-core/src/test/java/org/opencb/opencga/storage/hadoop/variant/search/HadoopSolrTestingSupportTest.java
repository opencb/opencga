package org.opencb.opencga.storage.hadoop.variant.search;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

@Category(ShortTests.class)
public class HadoopSolrTestingSupportTest {

    @Test
    public void testSupported() {
        VariantSolrExternalResource externalResource = new VariantSolrExternalResource();
        try {
            externalResource.before();
            externalResource.after();
            if (!HadoopVariantStorageTest.HadoopSolrSupport.isSolrTestingAvailable()) {
                Assert.fail("Solr testing should not be available");
            } else {
                System.out.println("As expected :: Solr testing available");
            }
        } catch (Throwable throwable) {
            if (HadoopVariantStorageTest.HadoopSolrSupport.isSolrTestingAvailable()) {
                Assert.fail("Solr testing should be available");
            } else {
                System.out.println("As expected :: Solr testing not available: " + throwable.getMessage());
            }
        }
    }

}
