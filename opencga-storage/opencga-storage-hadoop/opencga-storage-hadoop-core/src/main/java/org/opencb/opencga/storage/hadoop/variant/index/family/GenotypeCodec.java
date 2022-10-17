package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;

import java.util.Collection;

/**
 * Created on 03/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GenotypeCodec {

    public static final int NUM_CODES = 16;

    public static final byte HOM_REF_UNPHASED   = 0;  //  0/0
    public static final byte HET_REF_UNPHASED   = 1;  //  0/1
    public static final byte HOM_ALT_UNPHASED   = 2;  //  1/1

    public static final byte HOM_REF_PHASED     = 3;  //  0|0
    public static final byte HET_REF_01_PHASED  = 4;  //  0|1
    public static final byte HET_REF_10_PHASED  = 5;  //  1|0
    public static final byte HOM_ALT_PHASED     = 6;  //  1|1

    public static final byte HEMI_REF           = 7;  //  0
    public static final byte HEMI_ALT           = 8;  //  1

    public static final byte MULTI_HOM          = 9;  //  2/2
    public static final byte MULTI_HET          = 10; //  1/2, 0/2, ..
    public static final byte MISSING_HOM        = 11; //  ./.
    public static final byte MISSING_HET        = 12; //  ./0 ./1 ...
    public static final byte UNKNOWN            = 13; //  ?/?

    public static final byte DISCREPANCY_SIMPLE = 14; // 0/1 AND 1/1 , which whichever phase
    public static final byte DISCREPANCY_ANY    = 15; // Any other combination

    private static final String[] GENOTYPES = new String[]{
            "0/0",
            "0/1",
            "1/1",
            "0|0",
            "0|1",
            "1|0",
            "1|1",
            "0",
            "1",
            "2/2",
            "1/2",
            "./.",
            "./*",
            "?/?",
            "DISCREPANCY_SIMPLE",
            "DISCREPANCY_ANY",
    };

    // GT codes that refer to only one possible genotype
    private static final boolean[] AMBIGUOUS_GT_CODE = new boolean[]{
            false,  // HOM_REF_UNPHASED
            false,  // HET_REF_UNPHASED
            false,  // HOM_ALT_UNPHASED
            false,  // HOM_REF_PHASED
            false,  // HET_REF_01_PHASED
            false,  // HET_REF_10_PHASED
            false,  // HOM_ALT_PHASED
            false,  // HEMI_REF
            false,  // HEMI_ALT
            true,   // MULTI_HOM
            true,   // MULTI_HET
            true,   // MISSING_HOM
            true,   // MISSING_HET
            true,   // UNKNOWN
            true,   // DISCREPANCY_SIMPLE
            true,   // DISCREPANCY_ANY
    };

    public static byte[] split(byte code) {
        return new byte[]{(byte) (code >>> 4 & 0b00001111), (byte) (code & 0b00001111)};
    }

    public static byte join(byte fatherCode, byte motherCode) {
        return (byte) (fatherCode << 4 | motherCode);
    }

    public static byte encode(String fatherGenotype, String motherGenotype) {
        byte fatherCode = encode(fatherGenotype);
        byte motherCode = encode(motherGenotype);
        return join(fatherCode, motherCode);
    }

    public static byte encode(Collection<String> fatherGenotype, Collection<String> motherGenotype) {
        byte fatherCode = encode(fatherGenotype);
        byte motherCode = encode(motherGenotype);
        return join(fatherCode, motherCode);
    }

    public static byte encode(Collection<String> genotypes) {
        if (genotypes.size() == 1) {
            return encode(genotypes.iterator().next());
        } else {
            for (String genotype : genotypes) {
                byte encode = encode(genotype);
                if (encode >= MULTI_HOM || encode == HOM_REF_UNPHASED || encode == HOM_REF_PHASED) {
                    return DISCREPANCY_ANY;
                }
            }
            return DISCREPANCY_SIMPLE;
        }
    }

    public static byte encode(String genotype) {

        if (genotype == null || genotype.isEmpty()) {
            return UNKNOWN;
        }

        switch (genotype) {
            case "0/0":
                return HOM_REF_UNPHASED;
            case "0/1":
                return HET_REF_UNPHASED;
            case "1/1":
                return HOM_ALT_UNPHASED;

            case "0|0" :
                return HOM_REF_PHASED;
            case "0|1" :
                return HET_REF_01_PHASED;
            case "1|0" :
                return HET_REF_10_PHASED;
            case "1|1" :
                return HOM_ALT_PHASED;

            case "0":
                return HEMI_REF;
            case "1":
                return HEMI_ALT;

            case GenotypeClass.UNKNOWN_GENOTYPE:
            case GenotypeClass.NA_GT_VALUE:
                return UNKNOWN;

            case "./.":
            case ".":
            case ".|.":
                return MISSING_HOM;

            default:
                if (genotype.contains(".")) {
                    return MISSING_HET;
                } else {
                    try {
                        Genotype gt = new Genotype(genotype);
                        int[] alleles = gt.getAllelesIdx();
                        for (int i = 1; i < alleles.length; i++) {
                            if (alleles[i] != alleles[0]) {
                                return MULTI_HET;
                            }
                        }
                        return MULTI_HOM;
                    } catch (IllegalArgumentException e) {
                        return UNKNOWN;
                    }
                }
        }
    }

    public static String decode(int i) {
        return GENOTYPES[i];
    }

    public static Pair<String, String> decodeParents(byte code) {
        byte[] split = split(code);
        return Pair.of(decode(split[0]), decode(split[1]));
    }

    public static boolean isAmbiguousCode(int i) {
        return AMBIGUOUS_GT_CODE[i];
    }
}
