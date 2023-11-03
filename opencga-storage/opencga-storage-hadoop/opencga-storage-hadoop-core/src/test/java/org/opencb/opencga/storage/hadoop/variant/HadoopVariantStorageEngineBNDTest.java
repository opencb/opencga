package org.opencb.opencga.storage.hadoop.variant;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineBNDTest;

/**
 * Created on 26/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class HadoopVariantStorageEngineBNDTest extends VariantStorageEngineBNDTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @Override
    protected void loadFiles() throws Exception {
        super.loadFiles();
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
    }

}
