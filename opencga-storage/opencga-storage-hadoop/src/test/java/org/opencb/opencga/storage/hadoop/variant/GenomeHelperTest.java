package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;

/**
 * Created on 20/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GenomeHelperTest {

    public static final int CHUNK_SIZE = 200;
    private GenomeHelper genomeHelper;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        GenomeHelper.setChunkSize(conf, CHUNK_SIZE);
        genomeHelper = new GenomeHelper(conf);

    }

    @Test
    public void testBlockRowKey() throws Exception {
        Assert.assertEquals("2", genomeHelper.extractChromosomeFromBlockId("2_222"));
        Assert.assertEquals(222, genomeHelper.extractSliceFromBlockId("2_222").longValue());
        Assert.assertEquals(222 * CHUNK_SIZE, genomeHelper.extractPositionFromBlockId("2_222").longValue());
    }

    @Test
    public void testVariantRowKey() throws Exception {
        checkVariantRowKeyGeneration(new Variant("5", 21648, "A", "T"));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "AAAAAA", "T"));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "A", ""));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "AAT", "TTT"));
        checkVariantRowKeyGeneration(new Variant("X", 21648, "", "TTT"));
        checkVariantRowKeyGeneration(new Variant("MT", 21648, "", ""));
    }

    public void checkVariantRowKeyGeneration(Variant variant) {
        byte[] variantRowkey = genomeHelper.generateVariantRowKey(variant);
        Variant generatedVariant = genomeHelper.extractVariantFromVariantRowKey(variantRowkey);
        Assert.assertEquals(variant, generatedVariant);
    }
}
