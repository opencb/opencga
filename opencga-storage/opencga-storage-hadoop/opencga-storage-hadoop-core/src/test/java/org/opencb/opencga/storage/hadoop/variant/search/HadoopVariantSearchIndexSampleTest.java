package org.opencb.opencga.storage.hadoop.variant.search;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.search.SearchIndexSamplesTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(LongTests.class)
public class HadoopVariantSearchIndexSampleTest extends SearchIndexSamplesTest implements HadoopVariantStorageTest {

    @ClassRule(order = -10)
    public static HadoopSolrSupport solrSupport = new HadoopSolrSupport();

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    private int i = 0;

    @Override
    public void load() throws Exception {
        super.load();
        i++;
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
    }
}
