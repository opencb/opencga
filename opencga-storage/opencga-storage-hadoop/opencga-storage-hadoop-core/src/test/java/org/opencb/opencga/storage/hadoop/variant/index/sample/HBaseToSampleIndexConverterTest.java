package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.hadoop.hbase.CellUtil.createCell;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseToSampleIndexConverter.splitValue;

/**
 * Created on 01/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToSampleIndexConverterTest {

    @Test
    public void splitValueTest() {
        Assert.assertEquals(Arrays.asList("1234", "5678", "asdf", "qwerty"), splitValue(createCell(toBytes("Key"), toBytes("1234,5678,asdf,qwerty"))));
        Assert.assertEquals(Arrays.asList("1234", "5678", "asdf", "qwerty"), splitValue(createCell(toBytes("Key"), toBytes(",1234,5678,,asdf,qwerty,"))));
    }
}
