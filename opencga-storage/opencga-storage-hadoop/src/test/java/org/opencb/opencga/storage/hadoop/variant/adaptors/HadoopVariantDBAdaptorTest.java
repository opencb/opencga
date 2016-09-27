package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.junit.*;
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


    @Override
    @Ignore
    public void rank_gene() throws Exception {
        Assume.assumeTrue(false);
        super.rank_gene();
    }

    @Override
    @Ignore
    public void testExcludeFiles() {
        Assume.assumeTrue(false);
        super.testExcludeFiles();
    }

    @Override
    @Ignore
    public void testGetAllVariants_missingAllele() throws Exception {
        Assume.assumeTrue(false);
        super.testGetAllVariants_missingAllele();
    }

    @Override
    @Ignore
    public void groupBy_gene() throws Exception {
        Assume.assumeTrue(false);
        super.groupBy_gene();
    }

    @Override
    @Ignore
    public void testGetAllVariants_files() {
        Assume.assumeTrue(false);
        super.testGetAllVariants_files();
    }

    @Override
    @Ignore
    public void rank_ct() throws Exception {
        Assume.assumeTrue(false);
        super.rank_ct();
    }

    @Override
    @Ignore
    public void testGetAllVariants() {
        Assume.assumeTrue(false);
        super.testGetAllVariants();
    }

    @Override
    @Ignore
    public void testInclude() {
        Assume.assumeTrue(false);
        super.testInclude();
    }

    @Override
    @Ignore
    public void testGetAllVariants_genotypes() {
        Assume.assumeTrue(false);
        super.testGetAllVariants_genotypes();
    }

    @Override
    @Ignore
    public void testGetAllVariants_cohorts() throws Exception {
        Assume.assumeTrue(false);
        super.testGetAllVariants_cohorts();
    }

    @Override
    @Ignore
    public void testIterator() {
        Assume.assumeTrue(false);
        super.testIterator();
    }
}
