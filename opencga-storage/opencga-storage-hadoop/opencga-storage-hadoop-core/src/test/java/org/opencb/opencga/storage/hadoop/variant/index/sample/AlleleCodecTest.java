package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Test;

import static org.junit.Assert.*;

public class AlleleCodecTest {

    @Test
    public void codec() {

        String[] alleles = {"A", "C", "G", "T"};

        for (String ref : alleles) {
            for (String alt : alleles) {
                if (ref.equals(alt)) {
                    continue;
                }
                assertTrue(AlleleCodec.valid(ref, alt));
                byte code = AlleleCodec.encode(ref, alt);
//                System.out.println(ref + ", " + alt + " " + IndexUtils.byteToString(code));
                assertArrayEquals(new String[]{ref, alt}, AlleleCodec.decode(code));
            }
        }
    }

    @Test
    public void validAlleles() {
        assertTrue(AlleleCodec.validAllele("A"));
        assertTrue(AlleleCodec.validAllele("C"));
        assertTrue(AlleleCodec.validAllele("G"));
        assertTrue(AlleleCodec.validAllele("T"));

        assertFalse(AlleleCodec.validAllele(""));
        assertFalse(AlleleCodec.validAllele("N"));
        assertFalse(AlleleCodec.validAllele("Z"));
        assertFalse(AlleleCodec.validAllele("~"));
        assertFalse(AlleleCodec.validAllele("0"));

        assertTrue(AlleleCodec.valid("C", "A"));

        assertFalse(AlleleCodec.valid("A", "A"));
        assertFalse(AlleleCodec.valid("A", "N"));
        assertFalse(AlleleCodec.valid("", "C"));
        assertFalse(AlleleCodec.valid("A", ""));

    }

}