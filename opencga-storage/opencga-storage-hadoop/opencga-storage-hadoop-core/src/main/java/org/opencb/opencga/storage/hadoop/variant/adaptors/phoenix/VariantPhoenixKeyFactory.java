/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.VARIANT_ID;

/**
 * Created on 25/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixKeyFactory {

    protected static final String SV_ALTERNATE_SEPARATOR = "|";
    protected static final String SV_ALTERNATE_SEPARATOR_SPLIT = "\\" + SV_ALTERNATE_SEPARATOR;

    public static final Comparator<String> HBASE_KEY_CHROMOSOME_COMPARATOR = (c1, c2) -> Bytes.compareTo(
            VariantPhoenixKeyFactory.generateSimpleVariantRowKey(c1, 1, "N", "N"),
            VariantPhoenixKeyFactory.generateSimpleVariantRowKey(c2, 1, "N", "N"));

    public static byte[] generateVariantRowKey(String chrom, int position) {
        return generateSimpleVariantRowKey(chrom, position, "", "");
    }

    public static byte[] generateVariantRowKey(Variant var) {
        return generateVariantRowKey(var.getChromosome(), var.getStart(), var.getEnd(), var.getReference(), var.getAlternate(),
                var.getSv());
    }

    public static byte[] generateVariantRowKey(VariantAnnotation variantAnnotation) {
        byte[] bytesRowKey = null;
        if (variantAnnotation.getAdditionalAttributes() != null) {
            AdditionalAttribute additionalAttribute = variantAnnotation.getAdditionalAttributes().get(GROUP_NAME.key());
            if (additionalAttribute != null) {
                String variantString = additionalAttribute
                        .getAttribute()
                        .get(VARIANT_ID.key());
                if (StringUtils.isNotEmpty(variantString)) {
                    bytesRowKey = generateVariantRowKey(new Variant(variantString));
                }
            }
        }
        if (bytesRowKey == null) {
            bytesRowKey = VariantPhoenixKeyFactory.generateSimpleVariantRowKey(
                    variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                    variantAnnotation.getReference(), variantAnnotation.getAlternate());
        }
        return bytesRowKey;
    }

    /**
     * Generates a Row key based on Chromosome, position, ref and alt. <br>
     * <ul>
     * <li>Using {@link Region#normalizeChromosome(String)} to get standard chromosome
     * name
     * </ul>
     *
     * @param chrom    Chromosome name
     * @param position Genomic position
     * @param ref      Reference name
     * @param alt      Alt name
     * @return {@link String} Row key string
     */
    public static byte[] generateSimpleVariantRowKey(String chrom, int position, String ref, String alt) {
        return generateVariantRowKey(chrom, position, null, ref, alt, null);
    }

    /**
     * Generates a Row key based on Chromosome, start, end (optional), ref and alt. <br>
     * <ul>
     * <li>Using {@link Region#normalizeChromosome(String)} to get standard chromosome
     * name
     * </ul>
     *
     * @param chrom    Chromosome name
     * @param start    Genomic start
     * @param end      Genomic end
     * @param ref      Reference name
     * @param alt      Alt name
     * @param sv       Structural Variation
     * @return {@link String} Row key string
     */
    public static byte[] generateVariantRowKey(String chrom, int start, Integer end, String ref, String alt, StructuralVariation sv) {
        chrom = Region.normalizeChromosome(chrom);
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(chrom.length())
                + QueryConstants.SEPARATOR_BYTE_ARRAY.length
                + PUnsignedInt.INSTANCE.getByteSize()
                + PVarchar.INSTANCE.estimateByteSizeFromLength(ref.length());
        alt = buildSymbolicAlternate(ref, alt, end, sv);
        if (!alt.isEmpty()) {
            size += QueryConstants.SEPARATOR_BYTE_ARRAY.length
                    + PVarchar.INSTANCE.estimateByteSizeFromLength(alt.length());
        }

        byte[] rk = new byte[size];

        int offset = 0;
        offset += PVarchar.INSTANCE.toBytes(chrom, rk, offset);
        rk[offset++] = QueryConstants.SEPARATOR_BYTE;
        offset += PUnsignedInt.INSTANCE.toBytes(start, rk, offset);
        // Separator not needed. PUnsignedInt.INSTANCE.isFixedWidth() = true
        offset += PVarchar.INSTANCE.toBytes(ref, rk, offset);

        if (!alt.isEmpty()) {
            // If the alternate is not empty, separator between reference and alternate is required.
            rk[offset++] = QueryConstants.SEPARATOR_BYTE;
            offset += PVarchar.INSTANCE.toBytes(alt, rk, offset);
        }

//        assert offset == size;
        return rk;
    }

    public static String buildSymbolicAlternate(Variant v) {
        return buildSymbolicAlternate(v.getReference(), v.getAlternate(), v.getEnd(), v.getSv());
    }

    // visible for test
    public static String buildSymbolicAlternate(String reference, String alternate, Integer end, StructuralVariation sv) {
        if (sv != null) {
            if (!Allele.wouldBeSymbolicAllele(alternate.getBytes()) && emptyCiStartEnd(sv)) {
                // Skip non symbolic variants
                return alternate;
            }

            if (StructuralVariantType.TANDEM_DUPLICATION.equals(sv.getType())) {
                alternate = VariantBuilder.DUP_TANDEM_ALT;
            }

            alternate = alternate
                    + SV_ALTERNATE_SEPARATOR + end
                    + SV_ALTERNATE_SEPARATOR + (sv.getCiStartLeft() == null ? 0 : sv.getCiStartLeft())
                    + SV_ALTERNATE_SEPARATOR + (sv.getCiStartRight() == null ? 0 : sv.getCiStartRight())
                    + SV_ALTERNATE_SEPARATOR + (sv.getCiEndLeft() == null ? 0 : sv.getCiEndLeft())
                    + SV_ALTERNATE_SEPARATOR + (sv.getCiEndRight() == null ? 0 : sv.getCiEndRight());

            if (StringUtils.isNotEmpty(sv.getLeftSvInsSeq()) || StringUtils.isNotEmpty(sv.getRightSvInsSeq())) {
                alternate = alternate
                        + SV_ALTERNATE_SEPARATOR + sv.getLeftSvInsSeq()
                        + SV_ALTERNATE_SEPARATOR + sv.getRightSvInsSeq();
            }

        }
        return alternate;
    }

    protected static boolean emptyCiStartEnd(StructuralVariation sv) {
        return (sv.getCiStartLeft() == null || sv.getCiStartLeft() == 0)
                && (sv.getCiStartRight() == null || sv.getCiStartRight() == 0)
                && (sv.getCiEndLeft() == null || sv.getCiEndLeft() == 0)
                && (sv.getCiEndRight() == null || sv.getCiEndRight() == 0);
    }

    public static Pair<String, Integer> extractChrPosFromVariantRowKey(byte[] variantRowKey) {
        return extractChrPosFromVariantRowKey(variantRowKey, 0, variantRowKey.length);
    }

    public static Pair<String, Integer> extractChrPosFromVariantRowKey(byte[] variantRowKey, int offset, int length) {
        return extractChrPosFromVariantRowKey(variantRowKey, offset, length, false);
    }

    public static Pair<String, Integer> extractChrPosFromVariantRowKey(byte[] variantRowKey, int offset, int length,
                                                                       boolean addLeadingZeroes) {
        int chrPosSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0, offset);
        String chromosome = (String) PVarchar.INSTANCE.toObject(variantRowKey, offset, chrPosSeparator, PVarchar.INSTANCE);

        int position;
        if (addLeadingZeroes && length - chrPosSeparator - 1 < Integer.BYTES) {
            byte[] positionBytes = new byte[Integer.BYTES];
            System.arraycopy(variantRowKey, offset + chrPosSeparator + 1, positionBytes, 0, length - chrPosSeparator - 1);
            position = (Integer) PUnsignedInt.INSTANCE.toObject(positionBytes);
        } else {
            position = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey, chrPosSeparator + 1, Integer.BYTES, PUnsignedInt.INSTANCE);
        }
        return Pair.newPair(chromosome, position);
    }

    public static String extractChrFromVariantRowKey(byte[] variantRowKey, int offset, int length) {
        int chrPosSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0, offset);

        return (String) PVarchar.INSTANCE.toObject(variantRowKey, offset, chrPosSeparator, PVarchar.INSTANCE);
    }

    public static Variant extractVariantFromResultSet(ResultSet resultSet) {
        String chromosome = null;
        Integer start = null;
        String reference = null;
        String alternate = null;
        try {
            chromosome = resultSet.getString(VariantPhoenixSchema.VariantColumn.CHROMOSOME.column());
            start = resultSet.getInt(VariantPhoenixSchema.VariantColumn.POSITION.column());
            reference = resultSet.getString(VariantPhoenixSchema.VariantColumn.REFERENCE.column());
            alternate = resultSet.getString(VariantPhoenixSchema.VariantColumn.ALTERNATE.column());

            String type = resultSet.getString(VariantPhoenixSchema.VariantColumn.TYPE.column());

            return buildVariant(chromosome, start, reference, alternate, type);
        } catch (RuntimeException | SQLException e) {
            throw new IllegalStateException("Fail to parse variant: " + chromosome
                    + ':' + start
                    + ':' + (reference == null ? "-" : reference)
                    + ':' + (alternate == null ? "-" : alternate), e);
        }
    }

    public static Variant extractVariantFromVariantRowKey(byte[] variantRowKey) {
        int chrPosSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0);
        String chromosome = (String) PVarchar.INSTANCE.toObject(variantRowKey, 0, chrPosSeparator, PVarchar.INSTANCE);

        Integer intSize = PUnsignedInt.INSTANCE.getByteSize();
        int position = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey, chrPosSeparator + 1, intSize, PUnsignedInt.INSTANCE);
        int referenceOffset = chrPosSeparator + 1 + intSize;
        int refAltSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0, referenceOffset);
        String reference;
        String alternate;
        if (refAltSeparator < 0) {
            reference = (String) PVarchar.INSTANCE.toObject(variantRowKey, referenceOffset, variantRowKey.length - referenceOffset,
                    PVarchar.INSTANCE);
            alternate = "";
        } else {
            reference = (String) PVarchar.INSTANCE.toObject(variantRowKey, referenceOffset, refAltSeparator - referenceOffset,
                    PVarchar.INSTANCE);
            alternate = (String) PVarchar.INSTANCE.toObject(variantRowKey, refAltSeparator + 1,
                    variantRowKey.length - (refAltSeparator + 1), PVarchar.INSTANCE);
        }
        try {
            return buildVariant(chromosome, position, reference, alternate, null);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Fail to parse variant: " + chromosome
                    + ':' + position
                    + ':' + (reference == null ? "-" : reference)
                    + ':' + (alternate == null ? "-" : alternate)
                    + " from RowKey: " + Bytes.toStringBinary(variantRowKey), e);
        }
    }

    public static Variant buildVariant(String chromosome, int start, String reference, String alternate, String type) {

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

            if (ciStartL > 0) {
                builder.setCiStart(ciStartL, ciStartR);
            }

            if (ciEndL > 0) {
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
