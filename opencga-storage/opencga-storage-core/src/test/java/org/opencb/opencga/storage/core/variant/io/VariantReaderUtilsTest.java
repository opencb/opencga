/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        assertTrue(VariantReaderUtils.isGvcf("file.gvcf.gz"));
        assertTrue(VariantReaderUtils.isGvcf("file.genomes.vcf.gz"));
        assertFalse(VariantReaderUtils.isGvcf("gvcf.gz"));
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
        assertEquals("s1_variants.genomes.vcf.gz.file.json.gz", VariantReaderUtils.getMetaFromTransformedFile("s1_variants.genomes.vcf.gz.variants.avro.gz"));

        thrown.expect(IllegalArgumentException.class);
        VariantReaderUtils.getMetaFromTransformedFile("s1.genome.vcf.gz");
    }

    @Test
    public void validVariantsFile() {
        assertTrue(VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.avro.gz"));
        assertTrue(VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.avro"));
        assertTrue(VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.json"));
        assertTrue(VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.json.gz"));
        assertTrue(VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.proto.gz"));
        assertTrue(VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.proto"));
        assertFalse(VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.gz.txt"));
        assertFalse(VariantReaderUtils.isTransformedVariants("file.vcf.gz.variants.gz"));
        assertFalse(VariantReaderUtils.isTransformedVariants("file.vcf"));
        assertFalse(VariantReaderUtils.isTransformedVariants("s1_variants.genomes.vcf.gz"));


        assertTrue(VariantReaderUtils.isMetaFile("file.vcf.file.json.gz"));

        assertFalse(VariantReaderUtils.isMetaFile("file.vcf.file.json"));
        assertFalse(VariantReaderUtils.isMetaFile("file.vcf.file.avro.gz"));
        assertFalse(VariantReaderUtils.isMetaFile("file.vcf.file.avro"));
        assertFalse(VariantReaderUtils.isMetaFile("file.vcf.file.proto"));
    }


}