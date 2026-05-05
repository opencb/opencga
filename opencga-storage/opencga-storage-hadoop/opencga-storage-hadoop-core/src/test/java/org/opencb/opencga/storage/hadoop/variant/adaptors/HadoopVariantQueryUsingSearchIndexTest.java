package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUsingSearchIndexTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.util.Collections;

@Category(LongTests.class)
public class HadoopVariantQueryUsingSearchIndexTest extends VariantQueryUsingSearchIndexTest implements HadoopVariantStorageTest {

    @ClassRule(order = -10)
    public static HadoopSolrSupport solrSupport = new HadoopSolrSupport();

    @ClassRule(order = 5)
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Override
    @Test
    public void testGetAllVariants_geneTrait() {
        // Non-HPO gene traits not indexed in Solr. Run HPO-only.
        // FIXME : Phoenix array comparison bug — See https://issues.apache.org/jira/browse/PHOENIX-2952
        testGetAllVariants_geneTrait(false, Collections.singleton("17:7681412:C:G"));
    }

    @Override
    public void testGetAllVariants_missingAllele() throws Exception {
        Assume.assumeTrue(HadoopVariantDBAdaptorTest.MISSING_ALLELE);
        super.testGetAllVariants_missingAllele();
    }
}
