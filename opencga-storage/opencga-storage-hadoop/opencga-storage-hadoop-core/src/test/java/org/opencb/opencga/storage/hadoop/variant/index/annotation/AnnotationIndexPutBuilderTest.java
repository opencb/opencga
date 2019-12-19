package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AnnotationIndexPutBuilderTest {

    public static final byte[] FAMILY = {'0'};

    @Test
    public void testBuilder() {
        AnnotationIndexPutBuilder builder = new AnnotationIndexPutBuilder();
        for (int i = 0; i < 256; i++) {
            builder.add(new AnnotationIndexEntry(
                    (byte) i,
                    i % 2 == 0,
                    (short) i,
                    (byte) i,
                    new byte[Integer.bitCount((short) i)],
                    new byte[]{
                            (byte) ((i & 0b1100) >> 2),
                            (byte) (i & 0b0011)},
                    i % 2 == 0, (byte) i));
        }

        Put put = new Put(new byte[]{0});
        builder.buildAndReset(put, "0/1", FAMILY);

        Map<String, byte[]> map = put.getFamilyCellMap().get(FAMILY).stream()
                .collect(Collectors.toMap(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil::cloneValue));

        byte[] a = map.get("_A_0/1");
        byte[] bt = map.get("_BT_0/1");
        byte[] ct = map.get("_CT_0/1");
        byte[] pf = map.get("_PF_0/1");
//        byte[] ac = map.get("_AC_0/1");

//        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
//            System.out.println(entry.getKey());
//            for (byte b : entry.getValue()) {
//                System.out.print(IndexUtils.byteToString(b)+", ");
//            }
//            System.out.println("");
//        }

        assertEquals(256, a.length);
        assertEquals(256 / 2, pf.length);
        assertEquals(256 / 2, bt.length);
        assertEquals(256 / 2 * Short.BYTES, ct.length);


        int genicCount = 0;
        for (int i = 0; i < 256; i++) {
            assertEquals((byte) i, a[i]);
            if (i % 2 == 1) {
                assertEquals((byte) i, bt[genicCount]);
                assertEquals((short) i, Bytes.toShort(ct, genicCount * Short.BYTES, 2));
                genicCount++;
            }
            byte b = pf[i / 2];
//            System.out.println("-------");
//            System.out.println("i = " + i + ", " + IndexUtils.byteToString((byte) i));
//            System.out.println("b = " + b + ", " + IndexUtils.byteToString(b));
//            System.out.println("i%4 = " + i % 4);

            byte b1 =   (byte) ((b >>> (i % 2 * 4    )) & 0b11);
//            System.out.println("(b >>> (i % 2 * 4    )) & 0b11 = " + b + ", " + IndexUtils.byteToString(b1));
//            System.out.println("(b >>> ( " + (i % 2 * 4) + " )) & 0b11 = " + b + ", " + IndexUtils.byteToString(b1));

            byte b2 =   (byte) ((b >>> (i % 2 * 4 + 2)) & 0b11);
//            System.out.println("(b >>> (i % 2 * 4 + 2)) & 0b11 = " + b + ", " + IndexUtils.byteToString(b2));
//            System.out.println("(b >>> ( " + (i % 2 * 4 + 2) + " )) & 0b11 = " + b + ", " + IndexUtils.byteToString(b2));

//            System.out.println("(byte) (i & 0b1100) >> 2 " + IndexUtils.byteToString((byte) ((i & 0b1100) >> 2)));
//            System.out.println("(byte) (i & 0b0011)      " + IndexUtils.byteToString((byte) (i & 0b0011)));
            assertEquals((byte) (i & 0b1100) >> 2, b1);
            assertEquals((byte) (i & 0b0011), b2);
        }
    }

    @Test
    public void testOddNumberOfPopFreq() {
        int N = 3;
        AnnotationIndexPutBuilder builder = new AnnotationIndexPutBuilder();
        int pfValue = 0;
        for (int varIdx = 0; varIdx < 100; varIdx++) {
            builder.add(new AnnotationIndexEntry(
                    (byte) 0,
                    false,
                    (short) 0,
                    (byte) 0,
                    new byte[0],
                    new byte[]{
                            (byte) pfValue++,
                            (byte) pfValue++,
                            (byte) pfValue++},
                    false, (byte) 0));
        }

        Put put = new Put(new byte[]{0});
        builder.buildAndReset(put, "0/1", FAMILY);

        byte[] pf = put.getFamilyCellMap().get(FAMILY).stream()
                .collect(Collectors.toMap(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil::cloneValue)).get("_PF_0/1");

        pfValue = 0;
        for (int varIdx = 0; varIdx < 100; varIdx++) {
            for (int i = 0; i < N; i++) {

                int byteIdx = (varIdx * N + i) / 4;
                byte b = pf[byteIdx];
                int bitIdx = (varIdx * N + i) % 4 * AnnotationIndexConverter.POP_FREQ_SIZE;
                byte b1 = (byte) ((b >>> bitIdx) & 0b11);
                assertEquals((byte) (pfValue++ & 0b11), b1);
            }
        }


    }

}