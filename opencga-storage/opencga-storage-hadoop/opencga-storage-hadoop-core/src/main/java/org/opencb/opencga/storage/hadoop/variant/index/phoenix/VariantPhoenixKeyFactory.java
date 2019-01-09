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

package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;

import java.sql.ResultSet;
import java.sql.SQLException;

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
        alt = buildSVAlternate(alt, end, sv);
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

        if (!alt.isEmpty() || sv != null) {
            // If the alternate is not empty, separator between reference and alternate is required.
            // or
            // If we have SV, the alternate is not the last element, so we need the separator
            rk[offset++] = QueryConstants.SEPARATOR_BYTE;
        }
        if (!alt.isEmpty()) {
            offset += PVarchar.INSTANCE.toBytes(alt, rk, offset);
        }

//        assert offset == size;
        return rk;
    }

    // visible for test
    static String buildSVAlternate(String alternate, Integer end, StructuralVariation sv) {
        if (sv != null) {
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

    public static Variant extractVariantFromResultSet(ResultSet resultSet) {
        String chromosome = null;
        Integer start = null;
        String reference = null;
        String alternate = null;
        try {
            chromosome = resultSet.getString(VariantPhoenixHelper.VariantColumn.CHROMOSOME.column());
            start = resultSet.getInt(VariantPhoenixHelper.VariantColumn.POSITION.column());
            reference = resultSet.getString(VariantPhoenixHelper.VariantColumn.REFERENCE.column());
            alternate = resultSet.getString(VariantPhoenixHelper.VariantColumn.ALTERNATE.column());

            String type = resultSet.getString(VariantPhoenixHelper.VariantColumn.TYPE.column());

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

    private static Variant buildVariant(String chromosome, int start, String reference, String alternate, String type) {

        Integer end = null;
        int ciStartL = 0;
        int ciStartR = 0;
        int ciEndL = 0;
        int ciEndR = 0;
        String insSeqL = null;
        String insSeqR = null;
        if (alternate != null && alternate.contains(SV_ALTERNATE_SEPARATOR)) {
            String[] s = alternate.split(SV_ALTERNATE_SEPARATOR_SPLIT);
            alternate = s[0];
            end = Integer.parseInt(s[1]);
            ciStartL = Integer.parseInt(s[2]);
            ciStartR = Integer.parseInt(s[3]);
            ciEndL = Integer.parseInt(s[4]);
            ciEndR = Integer.parseInt(s[5]);

            if (s.length > 6) {
                insSeqL = s[6];
                insSeqR = s[7];
            }

            if (end == 0) {
                end = null;
            }
        }

        VariantBuilder builder = new VariantBuilder(chromosome, start, end, reference, alternate);

        if (insSeqL != null) {
            builder.setSvInsSeq(insSeqL, insSeqR);
        }

        if (end != null) {
            if (ciStartL > 0) {
                builder.setCiStart(ciStartL, ciStartR);
            }

            if (ciEndL > 0) {
                builder.setCiEnd(ciEndL, ciEndR);
            }
        }

        if (StringUtils.isNotBlank(type)) {
            builder.setType(VariantType.valueOf(type));
        }

        return builder.build();
    }
}
