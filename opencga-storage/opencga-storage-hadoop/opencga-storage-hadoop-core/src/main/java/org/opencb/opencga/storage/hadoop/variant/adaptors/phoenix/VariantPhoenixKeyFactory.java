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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.core.variant.index.VariantKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

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
public class VariantPhoenixKeyFactory extends VariantKeyFactory {

    public static final Integer UINT_SIZE = PUnsignedInt.INSTANCE.getByteSize();
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

    public static byte[] generateVariantRowKey(ResultSet resultSet) {
        String chromosome = null;
        Integer start = null;
        String reference = null;
        String alternate = null;
        try {
            chromosome = resultSet.getString(VariantPhoenixSchema.VariantColumn.CHROMOSOME.column());
            start = resultSet.getInt(VariantPhoenixSchema.VariantColumn.POSITION.column());
            reference = resultSet.getString(VariantPhoenixSchema.VariantColumn.REFERENCE.column());
            alternate = resultSet.getString(VariantPhoenixSchema.VariantColumn.ALTERNATE.column());

            return generateVariantRowKey(chromosome, start, null, reference, alternate, null);
        } catch (RuntimeException | SQLException e) {
            throw new IllegalStateException("Fail to generate row key from Phoenix result set: " + chromosome
                    + ':' + start
                    + ':' + (reference == null ? "-" : reference)
                    + ':' + (alternate == null ? "-" : alternate), e);
        }
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

    public static boolean mightHashAlleles(Variant variant) {
        int size = getSize(variant);
        return size > HConstants.MAX_ROW_LENGTH;
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
        alt = buildSymbolicAlternate(ref, alt, start, end, sv);
        int size = getSize(chrom, ref, alt);

        if (size > HConstants.MAX_ROW_LENGTH) {
            // This is a problem. The row key is too long.
            // Use hashCode for reference/alternate/SV fields
            ref = hashAllele(ref);
            alt = hashAllele(alt);
            size = getSize(chrom, ref, alt);
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

    private static int getSize(Variant variant) {
        String symbolicAlternate = buildSymbolicAlternate(variant);
        return getSize(variant.getChromosome(), variant.getReference(), symbolicAlternate);
    }

    private static int getSize(String chrom, String ref, String alt) {
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(chrom.length())
                + QueryConstants.SEPARATOR_BYTE_ARRAY.length
                + PUnsignedInt.INSTANCE.getByteSize()
                + PVarchar.INSTANCE.estimateByteSizeFromLength(ref.length());
        if (!alt.isEmpty()) {
            size += QueryConstants.SEPARATOR_BYTE_ARRAY.length
                    + PVarchar.INSTANCE.estimateByteSizeFromLength(alt.length());
        }
        return size;
    }

    public static String hashAllele(String ref) {
        return HASH_PREFIX + Integer.toString(ref.hashCode());
    }

    public static String buildAlleles(Variant v) {
        return v.getReference() + SV_ALTERNATE_SEPARATOR + buildSymbolicAlternate(v);
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

    public static Variant extractVariantFromResult(Result result) {
        byte[] variantRowKey = result.getRow();

        int chrPosSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0);
        int referenceOffset = chrPosSeparator + 1 + UINT_SIZE;
        if (variantRowKey.length > (referenceOffset + HASH_PREFIX_BYTES.length)
                && Bytes.equals(variantRowKey, referenceOffset, HASH_PREFIX_BYTES.length,
                HASH_PREFIX_BYTES, 0, HASH_PREFIX_BYTES.length)) {
            // The reference and alternate are hashed.
            // The type and alleles are stored in the result
            byte[] type = result.getValue(GenomeHelper.COLUMN_FAMILY_BYTES,
                    VariantPhoenixSchema.VariantColumn.TYPE.bytes());
            byte[] alleles = result.getValue(GenomeHelper.COLUMN_FAMILY_BYTES,
                    VariantPhoenixSchema.VariantColumn.ALLELES.bytes());
            return extractVariantFromVariantRowKey(variantRowKey, type, alleles);
        } else {
            return extractVariantFromVariantRowKey(variantRowKey, null, null);
        }
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

            String alleles = resultSet.getString(VariantPhoenixSchema.VariantColumn.ALLELES.column());
            String type = resultSet.getString(VariantPhoenixSchema.VariantColumn.TYPE.column());

            return buildVariant(chromosome, start, reference, alternate, type, alleles);
        } catch (RuntimeException | SQLException e) {
            throw new IllegalStateException("Fail to parse variant: " + chromosome
                    + ':' + start
                    + ':' + (reference == null ? "-" : reference)
                    + ':' + (alternate == null ? "-" : alternate), e);
        }
    }

    public static Variant extractVariantFromVariantRowKey(byte[] variantRowKey, byte[] type, byte[] alleles) {
        int chrPosSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0);
        String chromosome = (String) PVarchar.INSTANCE.toObject(variantRowKey, 0, chrPosSeparator, PVarchar.INSTANCE);

        int position = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey, chrPosSeparator + 1, UINT_SIZE, PUnsignedInt.INSTANCE);
        int referenceOffset = chrPosSeparator + 1 + UINT_SIZE;
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
        String typeStr = null;
        String alleleStr = null;
        if (type != null) {
            typeStr = (String) PVarchar.INSTANCE.toObject(type);
        }
        if (alleles != null) {
            alleleStr = (String) PVarchar.INSTANCE.toObject(alleles);
        }
        try {
            return buildVariant(chromosome, position, reference, alternate, typeStr, alleleStr);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Fail to parse variant: " + chromosome
                    + ':' + position
                    + ':' + (reference == null ? "-" : reference)
                    + ':' + (alternate == null ? "-" : alternate)
                    + " from RowKey: " + Bytes.toStringBinary(variantRowKey), e);
        }
    }

}
