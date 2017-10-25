package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorMultiFileTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

/**
 * Created on 24/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantDBAdaptorMultiFileTest extends VariantDBAdaptorMultiFileTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Override
    public void before() throws Exception {
        boolean wasLoaded = loaded;
        super.before();
        if (loaded && !wasLoaded) {
            VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor()), newOutputUri(getClass().getSimpleName()));
        }
    }

    @Override
    protected ObjectMap getOptions() {
        return new ObjectMap()
                .append(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true)
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
    }

}
