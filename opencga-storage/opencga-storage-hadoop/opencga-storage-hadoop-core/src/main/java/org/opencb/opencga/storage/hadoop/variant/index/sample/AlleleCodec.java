package org.opencb.opencga.storage.hadoop.variant.index.sample;

public class AlleleCodec {

    private static final String[] ALLELE_CODES = {"A", "C", "G", "T"};

    public static boolean valid(String ref, String alt) {
        return validAllele(ref)
                && validAllele(alt)
                && !ref.equals(alt);
    }

    public static boolean validAllele(String allele) {
        if (allele.length() == 1) {
            switch (allele.charAt(0)) {
                case 'A':
                case 'C':
                case 'G':
                case 'T':
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    public static byte encode(String ref, String alt) {
        byte refCode = encode(ref);
        byte altCode = encode(alt);
        return join(refCode, altCode);
    }

    public static String[] decode(byte refAltCode) {
        return new String[]{
                ALLELE_CODES[(refAltCode & 0b1100_0000) >>> 6],
                ALLELE_CODES[(refAltCode & 0b0011_0000) >>> 4],
        };
    }

    public static byte encode(String allele) {
        switch (allele.charAt(0)) {
            case 'A':
                return 0b00; // 0
            case 'C':
                return 0b01; // 1
            case 'G':
                return 0b10; // 2
            case 'T':
                return 0b11; // 3
            default:
                throw new IllegalArgumentException("Can not codify allele " + allele);
        }
    }

    public static byte join(byte refCode, byte altCode) {
        return (byte) (refCode << 6 | altCode << 4);
    }
}
