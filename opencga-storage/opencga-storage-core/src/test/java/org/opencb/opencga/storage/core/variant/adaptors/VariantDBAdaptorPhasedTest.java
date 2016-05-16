package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;

/**
 * Created on 13/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantDBAdaptorPhasedTest extends VariantStorageManagerTestUtils {


    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        VariantStorageManager variantStorageManager = getVariantStorageManager();
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), "DP,PS");
        runDefaultETL(getResourceUri("variant-test-phased.vcf"), variantStorageManager, newStudyConfiguration(), options);

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);
        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant.toJson());
        }

    }

    @Test
    public void queryPhased() throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);
        QueryResult<Variant> result;

        result = dbAdaptor.getPhased("1:819411:A:G", STUDY_NAME, "SAMPLE_1", new QueryOptions(), 1000);
        Assert.assertEquals(4, result.getNumResults());
        Assert.assertEquals("1:819320:A:C", result.getResult().get(0).toString());
        Assert.assertEquals("1:819411:A:G", result.getResult().get(1).toString());
        Assert.assertEquals("1:819651:A:G", result.getResult().get(2).toString());
        Assert.assertEquals("1:820211:T:C", result.getResult().get(3).toString());

        result = dbAdaptor.getPhased("1:819411:A:G", STUDY_NAME, "SAMPLE_1", new QueryOptions(), 100000000);
        Assert.assertEquals(4, result.getNumResults());
        Assert.assertEquals("1:819320:A:C", result.getResult().get(0).toString());
        Assert.assertEquals("1:819411:A:G", result.getResult().get(1).toString());
        Assert.assertEquals("1:819651:A:G", result.getResult().get(2).toString());
        Assert.assertEquals("1:820211:T:C", result.getResult().get(3).toString());


        result = dbAdaptor.getPhased("1:819411:A:G", STUDY_NAME, "SAMPLE_2", new QueryOptions(), 100000000);
        Assert.assertEquals(4, result.getNumResults());
        Assert.assertEquals("1:819411:A:G", result.getResult().get(0).toString());
        Assert.assertEquals("1:819651:A:G", result.getResult().get(1).toString());
        Assert.assertEquals("1:820211:T:C", result.getResult().get(2).toString());
        Assert.assertEquals("1:820811:G:C", result.getResult().get(3).toString());

        result = dbAdaptor.getPhased("1:819320:A:C", STUDY_NAME, "SAMPLE_2", new QueryOptions(), 100000000);
        Assert.assertEquals(0, result.getNumResults());

        result = dbAdaptor.getPhased("1:734964:T:C", STUDY_NAME, "SAMPLE_2", new QueryOptions(), 100000000);
        Assert.assertEquals(0, result.getNumResults());

    }

}
