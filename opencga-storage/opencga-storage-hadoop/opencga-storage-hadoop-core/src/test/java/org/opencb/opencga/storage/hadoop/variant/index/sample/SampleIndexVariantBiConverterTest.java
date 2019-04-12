package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created on 11/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexVariantBiConverterTest {

    private SampleIndexVariantBiConverter converter;

    @Before
    public void setUp() throws Exception {
        converter = new SampleIndexVariantBiConverter();
    }

    @Test
    public void testConvertSNV() {
        testConvertVariant("A", "C");
    }

    @Test
    public void testConvertINDEL() {
        testConvertVariant("A", "");
        testConvertVariant("", "C");
    }

    @Test
    public void testConvertMNV() {
        testConvertVariant("A", "CT");
    }

    @Test
    public void testConvertSYMBOLIC() {
        testConvertVariant("1:12345678<12345678<12345678-12345800<12345800<12345800:-:<DEL>");
    }

    protected void testConvertVariant(final String ref, final String alt) {
        testConvertVariant("1:12345678:" + ref + ":" + alt);
    }

    protected void testConvertVariant(String variantString) {
        Variant variant = new Variant(variantString);
        int batchStart = variant.getStart() - variant.getStart() % SampleIndexSchema.BATCH_SIZE;

        byte[] bytes = converter.toBytes(variant);
//        System.out.println("bytes = " + bytesToString(bytes));
        assertEquals(variant, converter.toVariant("1", batchStart, bytes));


        int offset = 7;
        bytes = new byte[200];
        converter.toBytes(variant, bytes, offset);
//        System.out.println("bytes = " + bytesToString(bytes));
        assertEquals(variant, converter.toVariant("1", batchStart, bytes, offset));
    }

    protected String bytesToString(byte[] bytes) {
        StringBuilder sb = new StringBuilder("[");
        for (byte aByte : bytes) {
            sb.append(StringUtils.leftPad(Integer.toHexString(aByte).toUpperCase(), 2, '0'));
            sb.append(' ');
        }
        sb.append("]");
        return sb.toString();
    }

    @Test
    public void testVariantsStream() {
        int batchStart = 12000000;
        int numVariants = 100;
        List<Variant> variants = new ArrayList<>(numVariants);
        for (int i = 0; i < numVariants; i++) {
            variants.add(new Variant("1:" + (batchStart + i * 10) + ":A:T"));
        }

        byte[] bytes = converter.toBytes(variants);
        byte[] bytes2 = Bytes.toBytes(variants.stream().map(Variant::toString).collect(Collectors.joining(",")));

        int offset = bytes.length + 10;
        byte[] bytes3 = new byte[bytes.length + offset];
        int length = converter.toBytes(variants, bytes3, offset);

        SampleIndexVariantBiConverter.SampleIndexVariantIterator iterator =
                converter.toVariantsIterator("1", batchStart, bytes, 0, bytes.length);
        SampleIndexVariantBiConverter.SampleIndexVariantIterator iterator2 =
                converter.toVariantsIterator("1", batchStart, bytes2, 0, bytes2.length);
        SampleIndexVariantBiConverter.SampleIndexVariantIterator iterator3 =
                converter.toVariantsIterator("1", batchStart, bytes3, offset, length);

        int i = 0;
        while (iterator.hasNext()) {
//            assertTrue(iterator.hasNext());
            assertTrue(iterator2.hasNext());
            assertTrue(iterator3.hasNext());

            assertEquals(i, iterator.nextIndex());
            assertEquals(i, iterator2.nextIndex());
            assertEquals(i, iterator3.nextIndex());
            if (i % 2 == 0) {
                iterator.skip();
                iterator2.skip();
                iterator3.skip();
            } else {
                assertEquals(variants.get(i), iterator.next());
                assertEquals(variants.get(i), iterator2.next());
                assertEquals(variants.get(i), iterator3.next());
            }
            i++;
        }

        assertFalse(iterator.hasNext());
        assertFalse(iterator2.hasNext());
        assertFalse(iterator3.hasNext());

        assertEquals(numVariants, i);

    }

    @Test
    public void testBatchSizeFitsIn24Bits() {
        assertTrue(SampleIndexSchema.BATCH_SIZE < 0xFFFFFF);
    }

    @Test
    public void testEncode24bitInteger() {
        byte[] bytes = new byte[7];
        int offset = 0;
        for (int i = 0; i <= 0xFFFFFF; i++) {
            assertEquals(3, converter.append24bitInteger(i, bytes, offset));
            assertEquals(i, converter.read24bitInteger(bytes, offset));
        }

        offset = 4;
        for (int i = 0; i <= 0xFFFFFF; i++) {
            assertEquals(3, converter.append24bitInteger(i, bytes, offset));
            assertEquals(i, converter.read24bitInteger(bytes, offset));
        }
    }

    @Test
    public void splitValueTest() {
        Assert.assertEquals(Arrays.asList("1234", "5678", "asdf", "qwerty"), SampleIndexVariantBiConverter.split(Bytes.toBytes("1234,5678,asdf,qwerty")));
        Assert.assertEquals(Arrays.asList("1234", "5678", "asdf", "qwerty"), SampleIndexVariantBiConverter.split(Bytes.toBytes(",1234,5678,,asdf,qwerty,")));
    }
}