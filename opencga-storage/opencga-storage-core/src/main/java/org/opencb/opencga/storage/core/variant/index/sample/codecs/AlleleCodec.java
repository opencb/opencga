package org.opencb.opencga.storage.core.variant.index.sample.codecs;

import org.opencb.biodata.tools.commons.BiConverter;

import java.util.*;

/**
 * Variant allele codec. Encode and decode any length allele into a shorter string.
 * <p>
 * If the allele is not a pure ACGT allele, it will be left as it is. It might contain the special characters [N*,].
 * Symbolic alleles will be left as they are.
 *
 */
public class AlleleCodec implements BiConverter<String, String> {

    private static final Map<String, Character> ALLELES_ENCODE;
    private static final String[] ALLELES_DECODE;
    private static final Set<Character> BASES = new HashSet<>(Arrays.asList('A', 'C', 'G', 'T'));
    // Chars that might be present in the alleles, and should be left as they are
    private static final Set<Character> SKIP_CHARS = new HashSet<>(Arrays.asList('N', '*', ','));
    // Chars that might be present in uncommon alleles. If present, the allele can't be encoded
    private static final Set<Character> INVALID_CHARS = new HashSet<>(Arrays.asList(
            '<', '>', // symbolic
            '[', ']', // breakend
            '.', // single breakend
            '|', '-' // Special chars used at VariantPhoenixKeyFactory#buildSymbolicAlternate
    ));

    private static final Set<Character> RESERVED_CHARS;


    static {
        ALLELES_ENCODE = new HashMap<>();
        RESERVED_CHARS = new HashSet<>();
        RESERVED_CHARS.addAll(BASES);
        RESERVED_CHARS.addAll(SKIP_CHARS);
        RESERVED_CHARS.addAll(INVALID_CHARS);

        char code = ' ';
        // Generate all possible combinations of 3 bases
        for (Character base1 : BASES) {
            for (Character base2 : BASES) {
                for (Character base3 : BASES) {
                    code++;
                    while (RESERVED_CHARS.contains(code)) {
                        code++;
                    }
                    ALLELES_ENCODE.put(base1 + "" + base2 + base3, code);
                }
            }
        }

        // Generate all possible combinations of 2 bases
        for (Character base1 : BASES) {
            for (Character base2 : BASES) {
                code++;
                while (RESERVED_CHARS.contains(code)) {
                    code++;
                }
                ALLELES_ENCODE.put(base1 + "" + base2, code);
            }
        }
        ALLELES_DECODE = new String[code + 1];
        for (Map.Entry<String, Character> entry : ALLELES_ENCODE.entrySet()) {
            if (ALLELES_DECODE[entry.getValue()] != null) {
                throw new IllegalStateException("Repeated code " + ((int) entry.getValue()));
            }
            ALLELES_DECODE[entry.getValue()] = entry.getKey();
        }
    }

    @Override
    public String to(String value) {
        return encode(value);
    }

    public String encode(String allele) {
        StringBuilder sb = new StringBuilder(allele.length());
        int i = 0;
        while (i < allele.length()) {
            char charAt = allele.charAt(i);
            if (INVALID_CHARS.contains(charAt)) {
                // Allele contains non accepted characters. Skip encode
                return allele;
            }
            if (i + 2 < allele.length()) {
                String sub = allele.substring(i, i + 3);
                Character c = ALLELES_ENCODE.get(sub);
                if (c != null) {
                    sb.append(c);
                    i += 3;
                    continue;
                }
            }
            if (i + 1 < allele.length()) {
                String sub = allele.substring(i, i + 2);
                Character c = ALLELES_ENCODE.get(sub);
                if (c != null) {
                    sb.append(c);
                    i += 2;
                    continue;
                }
            }
            sb.append(charAt);
            i++;
        }
        return sb.toString();
    }

    @Override
    public String from(String value) {
        return decode(value);
    }

    public String decode(String allele) {
        StringBuilder sb = new StringBuilder(allele.length() * 2);
        for (int i = 0; i < allele.length(); i++) {
            char charAt = allele.charAt(i);
            if (INVALID_CHARS.contains(charAt)) {
                // Allele contains non accepted characters. Skip decode
                return allele;
            }
            if (charAt >= ALLELES_DECODE.length) {
                sb.append(charAt);
            } else {
                String s = ALLELES_DECODE[charAt];
                if (s != null) {
                    sb.append(s);
                } else {
                    sb.append(charAt);
                }
            }
        }
        return sb.toString();
    }

}
