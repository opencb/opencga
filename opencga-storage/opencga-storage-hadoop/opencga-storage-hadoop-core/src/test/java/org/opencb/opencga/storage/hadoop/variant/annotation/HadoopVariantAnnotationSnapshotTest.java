package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationSnapshotTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

/**
 * Created on 25/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantAnnotationSnapshotTest extends VariantAnnotationSnapshotTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }
}
