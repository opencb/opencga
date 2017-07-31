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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;

/**
 * Created on 25/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixKeyFactory {

    public static byte[] generateVariantRowKey(String chrom, int position) {
        return generateVariantRowKey(chrom, position, "", "");
    }

    public static byte[] generateVariantRowKey(Variant var) {
        return generateVariantRowKey(var.getChromosome(), var.getStart(), var.getReference(), var.getAlternate());
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
    public static byte[] generateVariantRowKey(String chrom, int position, String ref, String alt) {
        chrom = Region.normalizeChromosome(chrom);
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(chrom.length())
                + QueryConstants.SEPARATOR_BYTE_ARRAY.length
                + PUnsignedInt.INSTANCE.estimateByteSize(position)
                + PVarchar.INSTANCE.estimateByteSizeFromLength(ref.length());
        if (!alt.isEmpty()) {
            size += QueryConstants.SEPARATOR_BYTE_ARRAY.length
                    + PVarchar.INSTANCE.estimateByteSizeFromLength(alt.length());
        }
        byte[] rk = new byte[size];

        int offset = 0;
        offset += PVarchar.INSTANCE.toBytes(chrom, rk, offset);
        rk[offset++] = QueryConstants.SEPARATOR_BYTE;
        offset += PUnsignedInt.INSTANCE.toBytes(position, rk, offset);
        // Separator not needed. PUnsignedInt.INSTANCE.isFixedWidth() = true
        offset += PVarchar.INSTANCE.toBytes(ref, rk, offset);
        if (!alt.isEmpty()) {
            // If the last element is null, don't require separator
            rk[offset++] = QueryConstants.SEPARATOR_BYTE;
            offset += PVarchar.INSTANCE.toBytes(alt, rk, offset);
        }
//        assert offset == size;
        return rk;
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
            return new Variant(chromosome, position, reference, alternate);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Problems creating variant using [chr:"
                    + chromosome + ", pos:" + position + ", ref:" + reference + ", alt:" + alternate + "];[hexstring:"
                    + Bytes.toHex(variantRowKey) + "]", e);
        }
    }
}
