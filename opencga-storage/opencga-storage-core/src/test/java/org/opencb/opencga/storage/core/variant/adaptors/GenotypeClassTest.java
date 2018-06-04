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

    @Test
    public void testGenotypes() throws Exception {
        List<String> gts = Arrays.asList("0/0", "0", "0/1", "1/1", "./.", ".", "1/2", "0/2", "2/2", "2/3", "./0", "0/.", "./1", "1/.");
        assertEquals(Arrays.asList("0/0", "0"), GenotypeClass.HOM_REF.filter(gts));
        assertEquals(Arrays.asList("1/1", "2/2"), GenotypeClass.HOM_ALT.filter(gts));
        assertEquals(Arrays.asList("0/1", "1/2", "0/2", "2/3"), GenotypeClass.HET.filter(gts));
        assertEquals(Arrays.asList("0/1", "0/2"), GenotypeClass.HET_REF.filter(gts));
        assertEquals(Arrays.asList("1/2", "2/3"), GenotypeClass.HET_ALT.filter(gts));
        assertEquals(Arrays.asList("./.", "."), GenotypeClass.MISS.filter(gts));
    }
}
