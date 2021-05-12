package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created on 04/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GenotypeClassTest {

    private final List<String> loadedGenotypes = Arrays.asList(
            "0/0", "0",
            "0/1", "1/1",
            "0|1", "1|0", "1|1",
            "./.", ".|.", ".",
            "1/2", "1|2", "./0", ".|0", "0|.", "0/.", "./1", "1/.");

    @Test
    public void testGenotypes() throws Exception {
        List<String> gts = Arrays.asList("0/0", "0", "0/1", "1/1", "./.", ".", "1/2", "0/2", "2/2", "2/3", "./0", "0/.", "./1", "1/.", "2/.");
        assertEquals(Arrays.asList("0/0", "0"), GenotypeClass.HOM_REF.filter(gts));
        assertEquals(Arrays.asList("1/1"), GenotypeClass.HOM_ALT.filter(gts));
        assertEquals(Arrays.asList("0/1", "1/2", "./1", "1/."), GenotypeClass.HET.filter(gts));
        assertEquals(Arrays.asList("0/1"), GenotypeClass.HET_REF.filter(gts));
        assertEquals(Arrays.asList("1/2"), GenotypeClass.HET_ALT.filter(gts));
        assertEquals(Arrays.asList("./1", "1/."), GenotypeClass.HET_MISS.filter(gts));
        assertEquals(Arrays.asList("./.", "."), GenotypeClass.MISS.filter(gts));
        assertEquals(Arrays.asList("0/2", "2/2", "2/3", "2/."), GenotypeClass.SEC_ALT.filter(gts));

    }

    @Test
    public void testPhasedGenotypes() throws Exception {
        assertEquals(Arrays.asList("0/1", "0|1", "1|0"), GenotypeClass.filter(Arrays.asList("0/1"), loadedGenotypes));
        assertEquals(Arrays.asList("!0/1", "!0|1", "!1|0"), GenotypeClass.filter(Arrays.asList("!0/1"), loadedGenotypes));
        assertEquals(Arrays.asList("0/1", "0|1", "1|0"), GenotypeClass.filter(Arrays.asList("1/0"), loadedGenotypes));
        assertEquals(Arrays.asList("0/1", "0|1", "1|0"), GenotypeClass.filter(Arrays.asList("0/1", "1/0"), loadedGenotypes));
        assertEquals(Arrays.asList("0|1"), GenotypeClass.filter(Arrays.asList("0|1"), loadedGenotypes));
        assertEquals(Arrays.asList("1|0"), GenotypeClass.filter(Arrays.asList("1|0"), loadedGenotypes));
        assertEquals(Arrays.asList("0"), GenotypeClass.filter(Arrays.asList("0"), loadedGenotypes));
        assertEquals(Arrays.asList("1"), GenotypeClass.filter(Arrays.asList("1"), loadedGenotypes));
    }
}
