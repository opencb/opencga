package org.opencb.opencga.storage.core.variant.index;

import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.StructuralVariantType;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.biodata.models.variant.avro.VariantType;

public class VariantKeyFactory {

    protected static final String SV_ALTERNATE_SEPARATOR = "|";
    protected static final String SV_ALTERNATE_SEPARATOR_SPLIT = "\\" + SV_ALTERNATE_SEPARATOR;
    public static final String HASH_PREFIX = "#";
    public static final byte[] HASH_PREFIX_BYTES = new byte[]{'#'};

    protected static boolean emptyCiStartEnd(StructuralVariation sv) {
        return (sv.getCiStartLeft() == null || sv.getCiStartLeft() == 0)
                && (sv.getCiStartRight() == null || sv.getCiStartRight() == 0)
                && (sv.getCiEndLeft() == null || sv.getCiEndLeft() == 0)
                && (sv.getCiEndRight() == null || sv.getCiEndRight() == 0);
    }

    public static String buildSymbolicAlternate(Variant v) {
        return buildSymbolicAlternate(v.getReference(), v.getAlternate(), v.getStart(), v.getEnd(), v.getSv());
    }

    // visible for test
    public static String buildSymbolicAlternate(String reference, String alternate, int start, Integer end, StructuralVariation sv) {
        if (sv != null) {
            byte[] alternateBytes = alternate.getBytes();
            if (!Allele.wouldBeSymbolicAllele(alternateBytes) && emptyCiStartEnd(sv)) {
                // Skip non symbolic variants
                return alternate;
            }

            if (StructuralVariantType.TANDEM_DUPLICATION.equals(sv.getType())) {
                alternate = VariantBuilder.DUP_TANDEM_ALT;
            }
            // Ignore CIEND on variants without an actual END. This includes Breakends and INSERTIONS
            // These variants are not expected to have CIEND. This is a redundant check, as the CIEND should be empty after normalization.
            boolean ignoreCiend = Allele.wouldBeBreakpoint(alternateBytes) || end < start;

            alternate = alternate
                    + SV_ALTERNATE_SEPARATOR + end
                    + SV_ALTERNATE_SEPARATOR + (sv.getCiStartLeft() == null ? 0 : sv.getCiStartLeft())
                    + SV_ALTERNATE_SEPARATOR + (sv.getCiStartRight() == null ? 0 : sv.getCiStartRight())
                    + SV_ALTERNATE_SEPARATOR + ((ignoreCiend || sv.getCiEndLeft() == null) ? 0 : sv.getCiEndLeft())
                    + SV_ALTERNATE_SEPARATOR + ((ignoreCiend || sv.getCiEndRight() == null) ? 0 : sv.getCiEndRight());

            if (StringUtils.isNotEmpty(sv.getLeftSvInsSeq()) || StringUtils.isNotEmpty(sv.getRightSvInsSeq())) {
                alternate = alternate
                        + SV_ALTERNATE_SEPARATOR + sv.getLeftSvInsSeq()
                        + SV_ALTERNATE_SEPARATOR + sv.getRightSvInsSeq();
            }

        }
        return alternate;
    }

    public static Variant buildVariant(String chromosome, int start, String reference, String alternate, String type, String alleles) {
        if ((reference != null && reference.startsWith(HASH_PREFIX)) || (alternate != null && alternate.startsWith(HASH_PREFIX))) {
            if (StringUtils.isNotEmpty(alleles)) {
                int i1 = alleles.indexOf(SV_ALTERNATE_SEPARATOR);
                reference = alleles.substring(0, i1);
                alternate = alleles.substring(i1 + SV_ALTERNATE_SEPARATOR.length());
            } else {
                throw new IllegalStateException("Reference and alternate are hashed, but alleles is empty!"
                        + " '" + chromosome + "' '" + start + "' '" + reference + "' '" + alternate + "'");
            }
        }

        if (alternate != null && alternate.length() > 5 && alternate.contains(SV_ALTERNATE_SEPARATOR)) {
            Integer end = null;
            int ciStartL = 0;
            int ciStartR = 0;
            int ciEndL = 0;
            int ciEndR = 0;
            String insSeqL = null;
            String insSeqR = null;

            // Build SV variant, with VariantBuilder
            String[] s = alternate.split(SV_ALTERNATE_SEPARATOR_SPLIT);
            alternate = s[0];
            end = s[1].equals("null") ? null : Integer.parseInt(s[1]);
            ciStartL = Integer.parseInt(s[2]);
            ciStartR = Integer.parseInt(s[3]);
            ciEndL = Integer.parseInt(s[4]);
            ciEndR = Integer.parseInt(s[5]);

            if (s.length > 6) {
                insSeqL = s[6];
                insSeqR = s[7];
            }

            if (end != null && end == 0) {
                end = null;
            }

            VariantBuilder builder = new VariantBuilder(chromosome, start, end, reference, alternate);
            builder.setSvInsSeq(insSeqL, insSeqR);

            if (ciStartL > 0 || ciStartR > 0) {
                builder.setCiStart(ciStartL, ciStartR);
            }

            if (ciEndL > 0 || ciEndR > 0) {
                builder.setCiEnd(ciEndL, ciEndR);
            }
            if (StringUtils.isNotBlank(type)) {
                builder.setType(VariantType.valueOf(type));
            }
            return builder.build();
        } else {
            // Build simple variant
            Variant variant = new Variant(chromosome, start, reference, alternate);
            if (StringUtils.isNotBlank(type)) {
                variant.setType(VariantType.valueOf(type));
            }
            return variant;
        }
    }

}
