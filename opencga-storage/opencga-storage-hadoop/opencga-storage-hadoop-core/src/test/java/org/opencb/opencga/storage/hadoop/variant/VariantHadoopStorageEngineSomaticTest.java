package org.opencb.opencga.storage.hadoop.variant;

import org.junit.After;
import org.junit.Rule;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSomaticTest;

/**
 * Created on 26/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopStorageEngineSomaticTest extends VariantStorageEngineSomaticTest implements HadoopVariantStorageTest {

    @Rule
    public HadoopExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }
}
