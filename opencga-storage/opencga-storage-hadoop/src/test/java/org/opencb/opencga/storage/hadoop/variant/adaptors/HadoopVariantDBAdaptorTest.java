package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManagerTestUtils;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;


/**
 * Created on 20/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantDBAdaptorTest extends VariantDBAdaptorTest implements HadoopVariantStorageManagerTestUtils {


    @Before
    @Override
    public void before() throws Exception {
        boolean fileIndexed = VariantDBAdaptorTest.fileIndexed;
        super.before();
        if (!fileIndexed) {
            VariantHbaseTestUtils.printVariantsFromVariantsTable((VariantHadoopDBAdaptor) dbAdaptor);
        }
    }


    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

//    @Override
//    protected ObjectMap getOtherParams() {
//        return new ObjectMap(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "proto")
//                .append(HadoopVariantStorageManager.HADOOP_LOAD_DIRECT, true)
//                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false);
//    }
    @Override
    protected ObjectMap getOtherParams() {
        return new ObjectMap(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "avro")
                .append(HadoopVariantStorageManager.HADOOP_LOAD_DIRECT, false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
    }
}
