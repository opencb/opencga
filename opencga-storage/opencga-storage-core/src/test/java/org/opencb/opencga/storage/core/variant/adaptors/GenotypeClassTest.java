package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Created on 04/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class GenotypeClassTest {

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
    public void classify() {
        checkClassify("0/0", GenotypeClass.HOM_REF);
        checkClassify("0|0", GenotypeClass.HOM_REF);
        checkClassify("0", GenotypeClass.HOM_REF);
        checkClassify("1/1", GenotypeClass.MAIN_ALT, GenotypeClass.HOM_ALT);
        checkClassify("1/1/1", GenotypeClass.MAIN_ALT, GenotypeClass.HOM_ALT);
        checkClassify("1", GenotypeClass.MAIN_ALT, GenotypeClass.HOM_ALT);
        checkClassify("0/1", GenotypeClass.MAIN_ALT, GenotypeClass.HET_REF, GenotypeClass.HET);
        checkClassify("1/0", GenotypeClass.MAIN_ALT, GenotypeClass.HET_REF, GenotypeClass.HET);
        checkClassify("0|1", GenotypeClass.MAIN_ALT, GenotypeClass.HET_REF, GenotypeClass.HET);
        checkClassify("1|0", GenotypeClass.MAIN_ALT, GenotypeClass.HET_REF, GenotypeClass.HET);
        checkClassify("1/2", GenotypeClass.MAIN_ALT, GenotypeClass.SEC, GenotypeClass.HET, GenotypeClass.HET_ALT);
        checkClassify("1/4", GenotypeClass.MAIN_ALT, GenotypeClass.SEC, GenotypeClass.HET, GenotypeClass.HET_ALT);
        checkClassify("3/4", GenotypeClass.SEC_ALT, GenotypeClass.SEC);
        checkClassify("3/4/5", GenotypeClass.SEC_ALT, GenotypeClass.SEC, GenotypeClass.HET);
        checkClassify("561/941", GenotypeClass.SEC_ALT, GenotypeClass.SEC);
        checkClassify("561/1", GenotypeClass.MAIN_ALT, GenotypeClass.SEC, GenotypeClass.HET, GenotypeClass.HET_ALT);
        checkClassify("0/2", GenotypeClass.SEC_ALT, GenotypeClass.SEC);
        checkClassify("0/3", GenotypeClass.SEC_ALT, GenotypeClass.SEC);
        checkClassify("3/3", GenotypeClass.SEC_ALT, GenotypeClass.SEC);
        checkClassify("1/.", GenotypeClass.MAIN_ALT, GenotypeClass.HET_MISS, GenotypeClass.HET);
        checkClassify("0/.");
        checkClassify("./.", GenotypeClass.MISS);
        checkClassify(".", GenotypeClass.MISS);
        checkClassify("NA", GenotypeClass.NA);
        checkClassify("THIS_IS_NOT_A_GENOTYPE");
    }

    private void checkClassify(String gt, GenotypeClass... expected) {
        Set<GenotypeClass> actual = GenotypeClass.classify(gt);
//        System.out.println("GenotypeClass.classify(" + gt + ") = " + actual);
        assertEquals(new HashSet<>(Arrays.asList(expected)), actual);
    }

    @Test
    public void testPhasedGenotypes() throws Exception {
        List<String> loadedGenotypes = Arrays.asList(
                "0/0", "0",
                "0/1", "1/1",
                "0|1", "1|0", "1|1",
                "./.", ".|.", ".",
                "1/2", "1|2", "./0", ".|0", "0|.", "0/.", "./1", "1/.");
        assertEquals(Arrays.asList("0/1", "0|1", "1|0"), GenotypeClass.filter(Arrays.asList("0/1"), loadedGenotypes));
        assertEquals(Arrays.asList("!0/1", "!0|1", "!1|0"), GenotypeClass.filter(Arrays.asList("!0/1"), loadedGenotypes));
        assertEquals(Arrays.asList("0/1", "0|1", "1|0"), GenotypeClass.filter(Arrays.asList("1/0"), loadedGenotypes));
        assertEquals(Arrays.asList("0/1", "0|1", "1|0"), GenotypeClass.filter(Arrays.asList("0/1", "1/0"), loadedGenotypes));
        assertEquals(Arrays.asList("0|1"), GenotypeClass.filter(Arrays.asList("0|1"), loadedGenotypes));
        assertEquals(Arrays.asList("1|0"), GenotypeClass.filter(Arrays.asList("1|0"), loadedGenotypes));
        assertEquals(Arrays.asList("0"), GenotypeClass.filter(Arrays.asList("0"), loadedGenotypes));
        assertEquals(Arrays.asList("1"), GenotypeClass.filter(Arrays.asList("1"), loadedGenotypes));
    }


    @Test
    public void testMultiAllelicGenotypes() throws Exception {
        List<String> gts = Arrays.asList("0/0", "0", "0/1", "1/1", "./.", ".", "1/2", "1/3", "1|3", "2|1", "0/2", "2/2", "3/3", "2/3",
                "2/4", "./0", "0/.", "./1", "1/.", "2/.");
        assertEquals(Arrays.asList("1|3", "2|1", "1/2", "1/3"), GenotypeClass.expandMultiAllelicGenotype("1/4", gts));

        assertEquals(Arrays.asList("2/2", "3/3", "2/3", "2/4"), GenotypeClass.expandMultiAllelicGenotype("2/2", gts));
//        assertEquals(Arrays.asList("2/2", "3/3"), GenotypeClass.expandMultiAllelicGenotype("2/2", gts));
//        assertEquals(Arrays.asList("2/3", "2/4"), GenotypeClass.expandMultiAllelicGenotype("2/3", gts));
    }
}
