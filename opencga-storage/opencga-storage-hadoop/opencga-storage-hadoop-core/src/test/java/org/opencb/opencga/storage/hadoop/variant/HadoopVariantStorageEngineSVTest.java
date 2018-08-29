package org.opencb.opencga.storage.hadoop.variant;

import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSVTest;

/**
 * Created on 26/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageEngineSVTest extends VariantStorageEngineSVTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @Test
    public void printVariants() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }
}
