package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Assert;
import org.junit.Test;

public class PhoenixHelperTest {

    @Test
    public void positionAtArrayElementTest() {
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(new PhoenixArray(PVarchar.INSTANCE, new String[]{"a", "b"}));
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
//        System.out.println("ptr = " + ptr);
        Assert.assertTrue(PhoenixHelper.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, null));
//        System.out.println("ptr = " + ptr);
    }
}