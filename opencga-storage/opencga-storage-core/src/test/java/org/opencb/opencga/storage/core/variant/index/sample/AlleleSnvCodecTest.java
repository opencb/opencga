package org.opencb.opencga.storage.core.variant.index.sample;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.index.sample.codecs.AlleleSnvCodec;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class AlleleSnvCodecTest {

    @Test
    public void codec() {

        String[] alleles = {"A", "C", "G", "T"};

        for (String ref : alleles) {
            for (String alt : alleles) {
                if (ref.equals(alt)) {
                    continue;
                }
                assertTrue(AlleleSnvCodec.valid(ref, alt));
                byte code = AlleleSnvCodec.encode(ref, alt);
//                System.out.println(ref + ", " + alt + " " + IndexUtils.byteToString(code));
                assertArrayEquals(new String[]{ref, alt}, AlleleSnvCodec.decode(code));
            }
        }
    }

    @Test
    public void validAlleles() {
        assertTrue(AlleleSnvCodec.validAllele("A"));
        assertTrue(AlleleSnvCodec.validAllele("C"));
        assertTrue(AlleleSnvCodec.validAllele("G"));
        assertTrue(AlleleSnvCodec.validAllele("T"));

        assertFalse(AlleleSnvCodec.validAllele(""));
        assertFalse(AlleleSnvCodec.validAllele("N"));
        assertFalse(AlleleSnvCodec.validAllele("Z"));
        assertFalse(AlleleSnvCodec.validAllele("~"));
        assertFalse(AlleleSnvCodec.validAllele("0"));

        assertTrue(AlleleSnvCodec.valid("C", "A"));

        assertFalse(AlleleSnvCodec.valid("A", "A"));
        assertFalse(AlleleSnvCodec.valid("A", "N"));
        assertFalse(AlleleSnvCodec.valid("", "C"));
        assertFalse(AlleleSnvCodec.valid("A", ""));

    }

}