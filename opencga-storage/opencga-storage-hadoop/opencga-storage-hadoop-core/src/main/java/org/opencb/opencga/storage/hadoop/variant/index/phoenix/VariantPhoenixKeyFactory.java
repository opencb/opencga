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
import org.opencb.biodata.models.variant.avro.StructuralVariantType;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.OPTIONAL_PRIMARY_KEY;

/**
 * Created on 25/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixKeyFactory {

    private static final int END_OFFSET = 0 * Bytes.SIZEOF_INT;
    private static final int CI_START_L_OFFSET = 1 * Bytes.SIZEOF_INT;
    private static final int CI_START_R_OFFSET = 2 * Bytes.SIZEOF_INT;
    private static final int CI_END_L_OFFSET = 3 * Bytes.SIZEOF_INT;
    private static final int CI_END_R_OFFSET = 4 * Bytes.SIZEOF_INT;
    private static final String INS_SEQ_SEPARATOR = "_";

    public static byte[] generateVariantRowKey(String chrom, int position) {
        return generateSimpleVariantRowKey(chrom, position, "", "");
    }

    public static byte[] generateVariantRowKey(Variant var) {
        return generateVariantRowKey(var.getChromosome(), var.getStart(), var.getEnd(), var.getReference(), var.getAlternate(),
                var.getSv());
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
        alt = buildSVAlternate(alt, sv);
        if (!alt.isEmpty()) {
            size += QueryConstants.SEPARATOR_BYTE_ARRAY.length
                    + PVarchar.INSTANCE.estimateByteSizeFromLength(alt.length());
        }
        if (sv != null) {
            if (alt.isEmpty()) {
                // If alt is empty, add separator
                size += QueryConstants.SEPARATOR_BYTE_ARRAY.length;
            }
            size += PUnsignedInt.INSTANCE.getByteSize() * OPTIONAL_PRIMARY_KEY.size() + QueryConstants.SEPARATOR_BYTE_ARRAY.length;
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
        if (sv != null) {
            rk[offset++] = QueryConstants.SEPARATOR_BYTE;
            offset += PUnsignedInt.INSTANCE.toBytes(end, rk, offset);
            offset += PUnsignedInt.INSTANCE.toBytes(sv.getCiStartLeft() == null ? 0 : sv.getCiStartLeft(), rk, offset);
            offset += PUnsignedInt.INSTANCE.toBytes(sv.getCiStartRight() == null ? 0 : sv.getCiStartRight(), rk, offset);
            offset += PUnsignedInt.INSTANCE.toBytes(sv.getCiEndLeft() == null ? 0 : sv.getCiEndLeft(), rk, offset);
            offset += PUnsignedInt.INSTANCE.toBytes(sv.getCiEndRight() == null ? 0 : sv.getCiEndRight(), rk, offset);
        }

//        assert offset == size;
        return rk;
    }

    private static String buildSVAlternate(String alternate, StructuralVariation sv) {
        if (sv != null) {
            if (StringUtils.isNotEmpty(sv.getLeftSvInsSeq()) || StringUtils.isNotEmpty(sv.getRightSvInsSeq())) {
                alternate = alternate + INS_SEQ_SEPARATOR + sv.getLeftSvInsSeq() + INS_SEQ_SEPARATOR + sv.getRightSvInsSeq();
            } else if (StructuralVariantType.TANDEM_DUPLICATION.equals(sv.getType())) {
                alternate = VariantBuilder.DUP_TANDEM_ALT;
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

            Integer end = resultSet.getInt(VariantPhoenixHelper.VariantColumn.SV_END.column());
            if (end == 0) {
                end = null;
            }

            String type = resultSet.getString(VariantPhoenixHelper.VariantColumn.TYPE.column());

            int ciStartL = resultSet.getInt(VariantPhoenixHelper.VariantColumn.CI_START_L.column());
            int ciStartR = resultSet.getInt(VariantPhoenixHelper.VariantColumn.CI_START_R.column());
            int ciEndL = resultSet.getInt(VariantPhoenixHelper.VariantColumn.CI_END_L.column());
            int ciEndR = resultSet.getInt(VariantPhoenixHelper.VariantColumn.CI_END_R.column());

            return buildVariant(chromosome, start, end, reference, alternate, type, ciStartL, ciStartR, ciEndL, ciEndR);
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
        Integer end = null;
        int ciStartL = 0;
        int ciStartR = 0;
        int ciEndL = 0;
        int ciEndR = 0;
        if (refAltSeparator < 0) {
            reference = (String) PVarchar.INSTANCE.toObject(variantRowKey, referenceOffset, variantRowKey.length - referenceOffset,
                    PVarchar.INSTANCE);
            alternate = "";
        } else {
            int altSvSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0, refAltSeparator + 1);
            reference = (String) PVarchar.INSTANCE.toObject(variantRowKey, referenceOffset, refAltSeparator - referenceOffset,
                    PVarchar.INSTANCE);

            alternate = (String) PVarchar.INSTANCE.toObject(variantRowKey, refAltSeparator + 1,
                    (altSvSeparator == -1 ? variantRowKey.length : altSvSeparator) - (refAltSeparator + 1), PVarchar.INSTANCE);
            if (altSvSeparator > 0) {
                int offset = altSvSeparator + 1; // Advance offset to point just after the separator
                end = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey,
                        offset + END_OFFSET, Bytes.SIZEOF_INT, PUnsignedInt.INSTANCE);
                ciStartL = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey,
                        offset + CI_START_L_OFFSET, Bytes.SIZEOF_INT, PUnsignedInt.INSTANCE);
                ciStartR = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey,
                        offset + CI_START_R_OFFSET, Bytes.SIZEOF_INT, PUnsignedInt.INSTANCE);
                ciEndL = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey,
                        offset + CI_END_L_OFFSET, Bytes.SIZEOF_INT, PUnsignedInt.INSTANCE);
                ciEndR = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey,
                        offset + CI_END_R_OFFSET, Bytes.SIZEOF_INT, PUnsignedInt.INSTANCE);
            }
        }
        try {
            return buildVariant(chromosome, position, end, reference, alternate, null, ciStartL, ciStartR, ciEndL, ciEndR);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Fail to parse variant: " + chromosome
                    + ':' + position
                    + ':' + (reference == null ? "-" : reference)
                    + ':' + (alternate == null ? "-" : alternate)
                    + " from RowKey: " + Bytes.toStringBinary(variantRowKey), e);
        }
    }

    private static Variant buildVariant(String chromosome, int start, Integer end, String reference, String alternate, String type,
                                        int ciStartL, int ciStartR, int ciEndL, int ciEndR) {
        VariantBuilder builder = new VariantBuilder(chromosome, start, end, reference, alternate);

        if (alternate != null && alternate.contains(INS_SEQ_SEPARATOR)) {
            String[] alternateSplit = alternate.split(INS_SEQ_SEPARATOR);
            alternate = alternateSplit[0];

            builder.setAlternate(alternate);
            builder.setSvInsSeq(alternateSplit[1], alternateSplit[2]);
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
