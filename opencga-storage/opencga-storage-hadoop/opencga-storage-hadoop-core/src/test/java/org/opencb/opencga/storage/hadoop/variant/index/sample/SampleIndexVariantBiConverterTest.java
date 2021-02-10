package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.*;

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
    public void testConvertSNV() throws Exception {
        testConvertVariant("A", "C", false, 3);
    }

    @Test
    public void testConvertSNVBackwardCompatibility() {
        byte[] bytes = {
                0x05, 0x46, 0x4E, 'A', 0x00, 'C',
                0x00,
                0x05, 0x46, 0x4E, 'A', 0x00, 'C',};

        List<Variant> variants = converter.toVariants("1", 12000000, bytes, 0, bytes.length);
        assertEquals(new Variant("1:12345678:A:C"), variants.get(0));
        assertEquals(new Variant("1:12345678:A:C"), variants.get(0));
    }

    @Test
    public void testConvertINDEL() throws Exception {
        testConvertVariant("A", "", true, 5);
        testConvertVariant("", "C", true, 5);
    }

    @Test
    public void testConvertMNV() throws Exception {
        testConvertVariant("A", "CT", true, 7);
    }

    @Test
    public void testConvertSYMBOLIC() throws Exception {
        testConvertVariant("1:12345678<12345678<12345678-12345800<12345800<12345800:-:<DEL>", true, null);
        testConvertVariant("1:12345678<12345678<12345678:A:AAT]4:90265517]", true, null);
        testConvertVariant("1:12345678<12345678<12345678:-:AT]4:90265517]", true, null);
        testConvertVariant("1:12345678<12345678<12345678:-:.]4:90265517]", true, null);
    }

    protected void testConvertVariant(final String ref, final String alt, boolean withSeparator, Integer expectedLength) throws Exception {
        testConvertVariant("1:12345678:" + ref + ":" + alt, withSeparator, expectedLength);
    }

    protected void testConvertVariant(String variantString, boolean withSeparator, final Integer expectedLength) throws Exception {
        final Variant variant = new Variant(variantString);
        final int batchStart = variant.getStart() - variant.getStart() % SampleIndexSchema.BATCH_SIZE;
        final String chromosome = "1";

        // Test single variant convert
        byte[] bytes = converter.toBytes(variant);
        assertEquals(variant, converter.toVariant(chromosome, batchStart, bytes));

        final int actualLength = bytes.length;
        if (expectedLength != null) {
            assertEquals(expectedLength.intValue(), actualLength);
        }

        // Test convert in byte array
        final int offset = 7;
        bytes = new byte[200];
        converter.toBytes(Arrays.asList(variant, variant), bytes, offset);
        List<Variant> variants = converter.toVariants(chromosome, batchStart, bytes, offset, actualLength * 2 + (withSeparator ? 1 : 0));
        assertEquals(variant, variants.get(0));
        assertEquals(variant, variants.get(1));
        assertEquals(variant.toString(), variants.get(0).toString());
        assertEquals(variant.toString(), variants.get(1).toString());

        // Test convert in byte stream
        ByteArrayOutputStream os = new ByteArrayOutputStream(200);
        os.write(new byte[offset]); // add offset
        converter.toBytes(variant, os);
        converter.toBytes(variant, os);
        bytes = os.toByteArray();
        assertEquals(bytes.length, offset + (actualLength + (withSeparator ? 1 : 0)) * 2);
        variants = converter.toVariants(chromosome, batchStart, bytes, offset, os.size() - offset);
        assertEquals(variant, variants.get(0));
        assertEquals(variant, variants.get(1));
        assertEquals(variant.toString(), variants.get(0).toString());
        assertEquals(variant.toString(), variants.get(1).toString());

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
    public void testVariantsStream() throws IOException {
        int batchStart = 12000000;
        int numVariants = 100;
        List<Variant> variants = new ArrayList<>(numVariants);
        for (int i = 0; i < numVariants; i++) {
            variants.add(new Variant("1:" + (batchStart + i * 10) + ":A:T"));
        }

        byte[] bytes = converter.toBytes(variants);
        checkIterator(numVariants, variants, () -> toSampleIndexEntry(batchStart, bytes, 0, bytes.length));

        int bytes3offset = bytes.length + 10;
        byte[] bytes3 = new byte[bytes.length + bytes3offset];
        int length = converter.toBytes(variants, bytes3, bytes3offset);
        checkIterator(numVariants, variants, () -> toSampleIndexEntry(batchStart, bytes3, bytes3offset, length));


//        byte[] bytesOld = converter.toBytesSimpleString(variants);
//        checkIterator(numVariants, variants, () -> toSampleIndexEntry(batchStart, bytesOld, 0, bytesOld.length));

//        byte[] bytesOldOffset = new byte[bytesOld.length + 10];
//        System.arraycopy(bytesOld, 0, bytesOldOffset, 10, bytesOld.length);
//        checkIterator(numVariants, variants, () -> toSampleIndexEntry(batchStart, bytesOldOffset, 10, bytesOld.length));


        List<Variant> dummyVariants = new ArrayList<>(numVariants);
        for (int i = 0; i < numVariants; i++) {
            dummyVariants.add(new Variant("1:10:A:T"));
        }
        checkIterator(numVariants, dummyVariants, () -> {
            SampleIndexEntry entry = new SampleIndexEntry(0, "1", batchStart, SampleIndexConfiguration.defaultConfiguration());
            SampleIndexEntry.SampleIndexGtEntry gtEntry = entry.getGtEntry("0/1");
            return gtEntry.setCount(variants.size());
        }, true);

    }

    private SampleIndexEntry.SampleIndexGtEntry toSampleIndexEntry(int batchStart, byte[] bytes, int offset, int length) {
        SampleIndexEntry entry = new SampleIndexEntry(0, "1", batchStart, SampleIndexConfiguration.defaultConfiguration());
        SampleIndexEntry.SampleIndexGtEntry gtEntry = entry.getGtEntry("0/1");
        gtEntry.setVariants(bytes, offset, length);
        return gtEntry;
    }

    private void checkIterator(int numVariants, List<Variant> variants, Supplier<SampleIndexEntry.SampleIndexGtEntry> factory) {
        checkIterator(numVariants, variants, factory, false);
    }

    private void checkIterator(int numVariants, List<Variant> variants, Supplier<SampleIndexEntry.SampleIndexGtEntry> factory, boolean onlyCount) {
        checkIterator(numVariants, variants, factory.get(), false, onlyCount);
        checkIterator(numVariants, variants, factory.get(), true, onlyCount);
    }

    private void checkIterator(int numVariants, List<Variant> variants, SampleIndexEntry.SampleIndexGtEntry entry, boolean annotated, boolean onlyCount) {
        if (annotated) {
            byte[] annot = new byte[numVariants];
            for (int i = 0; i < numVariants; i++) {
                if (i % 3 != 0) {
                    annot[i] = AnnotationIndexConverter.INTERGENIC_MASK;
                }
            }
            entry.setAnnotationIndex(annot);
        }
        SampleIndexEntryIterator iterator = entry.iterator(onlyCount);
        int i = 0;
        int nonIntergenicIndex = 0;
        while (iterator.hasNext()) {
            assertEquals(i, iterator.nextIndex());
            if (annotated) {
                if (i % 3 == 0) {
                    assertEquals(nonIntergenicIndex++, iterator.nextNonIntergenicIndex());
                } else {
                    try {
                        iterator.nextNonIntergenicIndex();
                        fail("Expect IllegalStateException!");
                    } catch (IllegalStateException e) {
                        assertEquals("Next variant is not intergenic!", e.getMessage());
                    }
                }
            } else {
                assertEquals(-1, iterator.nextNonIntergenicIndex());
            }
            if (i % 2 == 0) {
                iterator.skip();
            } else {
                assertEquals(variants.get(i), iterator.next());
            }
            i++;
        }
        assertFalse(iterator.hasNext());
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