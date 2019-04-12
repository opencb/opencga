package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec.*;

/**
 * Created on 04/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GenotypeCodecTest {

    @Test
    public void testEncode() {

        assertEquals(HOM_REF_UNPHASED, encode("0/0"));
        assertEquals(HET_REF_UNPHASED, encode("0/1"));
        assertEquals(HOM_ALT_UNPHASED, encode("1/1"));
        assertEquals(HOM_REF_PHASED, encode("0|0"));
        assertEquals(HET_REF_01_PHASED, encode("0|1"));
        assertEquals(HET_REF_10_PHASED, encode("1|0"));
        assertEquals(HOM_ALT_PHASED, encode("1|1"));

        assertEquals(HEMI_REF, encode("0"));
        assertEquals(HEMI_ALT, encode("1"));

        assertEquals(MULTI_HOM, encode("2/2"));
        assertEquals(MULTI_HOM, encode("3/3"));
        assertEquals(MULTI_HOM, encode("2"));

        assertEquals(MULTI_HET, encode("1/2"));
        assertEquals(MULTI_HET, encode("1/5"));
        assertEquals(MULTI_HET, encode("0/2"));

        assertEquals(MISSING_HOM, encode("."));
        assertEquals(MISSING_HOM, encode("./."));
        assertEquals(MISSING_HET, encode("./0"));
        assertEquals(MISSING_HET, encode("./1"));
        assertEquals(UNKNOWN, encode("?/?"));
        assertEquals(UNKNOWN, encode("NA"));
        assertEquals(UNKNOWN, encode("asdf"));

    }

    @Test
    public void testEncodeParents() {
        for (byte fatherCode = 0; fatherCode < NUM_CODES; fatherCode++) {
            for (byte motherCode = 0; motherCode < NUM_CODES; motherCode++) {
                byte[] split = split(join(fatherCode, motherCode));
                assertEquals(fatherCode, split[0]);
                assertEquals(motherCode, split[1]);
            }
        }
    }
}