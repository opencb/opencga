package org.opencb.opencga.storage.hadoop.variant.search;

import org.junit.Rule;
import org.opencb.opencga.storage.core.variant.search.VariantSearchIndexTest;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchIndexTest extends VariantSearchIndexTest implements HadoopVariantStorageTest {

    @Rule
    public HadoopExternalResource externalResource = new HadoopExternalResource();

    private int i = 0;

    @Override
    public VariantSearchLoadResult searchIndex() throws Exception {
        i++;
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri("searchIndex_" + i + "_pre"));

        externalResource.flush(dbAdaptor.getVariantTable());
        VariantSearchLoadResult loadResult = variantStorageEngine.searchIndex();
        externalResource.flush(dbAdaptor.getVariantTable());
        System.out.println("[" + i + "] VariantSearch LoadResult " + loadResult);
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri("searchIndex_" + i + "_post"));
        return loadResult;
    }
}
