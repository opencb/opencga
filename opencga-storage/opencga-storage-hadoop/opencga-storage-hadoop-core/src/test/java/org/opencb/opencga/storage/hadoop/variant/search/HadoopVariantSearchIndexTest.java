package org.opencb.opencga.storage.hadoop.variant.search;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.search.VariantSearchIndexTest;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(LongTests.class)
public class HadoopVariantSearchIndexTest extends VariantSearchIndexTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopSolrSupport solrSupport = new HadoopSolrSupport();

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    private int i = 0;

    @Override
    public VariantSearchLoadResult searchIndex(boolean overwrite) throws Exception {
        i++;
        VariantHadoopDBAdaptor dbAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor();
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri("searchIndex_" + TimeUtils.getTime() + "_" + i + "_pre"));

        externalResource.flush(dbAdaptor.getVariantTable());
        VariantSearchLoadResult loadResult = super.searchIndex(overwrite);
        externalResource.flush(dbAdaptor.getVariantTable());
        System.out.println("[" + i + "] VariantSearch LoadResult " + loadResult);
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri("searchIndex_" + TimeUtils.getTime() + "_" + i + "_post"));
        return loadResult;
    }
}
