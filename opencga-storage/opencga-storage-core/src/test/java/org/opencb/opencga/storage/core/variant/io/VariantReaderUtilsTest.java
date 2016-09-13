package org.opencb.opencga.storage.core.variant.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

/**
 * Created on 12/08/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantReaderUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void formatTest() {
        assertTrue(VariantReaderUtils.isProto("s1.genome.vcf.variants.proto.gz"));
        assertTrue(VariantReaderUtils.isJson("fileName.json"));
        assertTrue(VariantReaderUtils.isJson("fileName.json.gz"));
        assertFalse(VariantReaderUtils.isJson("fileName.vcf.gz"));
        assertFalse(VariantReaderUtils.isJson("fileName.json.vcf.gz"));
        assertFalse(VariantReaderUtils.isJson("json"));
        assertFalse(VariantReaderUtils.isJson("json.gz"));
    }


    @Test
    public void getMetaTest() {
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.json.gz"));
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.json.snappy"));
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.json"));
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.avro.gz"));
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.avro.snappy"));
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.avro"));
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.proto.gz"));
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.proto.snappy"));
        assertEquals("s1.genome.vcf.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.variants.proto"));

        thrown.expect(IllegalArgumentException.class);
        VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.gz");
    }

    @Test
    public void validVariantsFile() {
        assertEquals(true, VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.avro.gz"));
        assertEquals(true, VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.avro"));
        assertEquals(true, VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.json"));
        assertEquals(true, VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.json.gz"));
        assertEquals(true, VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.proto.gz"));
        assertEquals(true, VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.proto"));
        assertEquals(false, VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.gz.txt"));
        assertEquals(false, VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.gz"));
        assertEquals(false, VariantReaderUtils.isTransformedVariants("file.vcf"));


        assertEquals(true, VariantReaderUtils.isMetaFile("file.vcf.file.json.gz"));
        assertEquals(false, VariantReaderUtils.isMetaFile("file.vcf.file.json"));
        assertEquals(false, VariantReaderUtils.isMetaFile("file.vcf.file.avro.gz"));
        assertEquals(false, VariantReaderUtils.isMetaFile("file.vcf.file.avro"));
        assertEquals(false, VariantReaderUtils.isMetaFile("file.vcf.file.proto"));
    }


}