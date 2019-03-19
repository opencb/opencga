package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

/**
 * Created on 01/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IndexUtils {

    public static final byte EMPTY_MASK = 0;
    private static final double DELTA = 0.000001;

    public static String byteToString(byte b) {
        String str = Integer.toBinaryString(b);
        if (str.length() > 8) {
            str = str.substring(str.length() - 8);
        }
        return StringUtils.leftPad(str, 8, '0');
    }

    public static String maskToString(byte[] maskAndIndex) {
        return maskToString(maskAndIndex[0], maskAndIndex[1]);
    }

    /**
     * Merge the mask and the index into one single string.
     *
     * Mask   : 00101110
     * Index  : 00100100
     * Result : **1*010*
     *
     * @param mask      Mask
     * @param index     Index
     * @return      String representation
     */
    public static String maskToString(byte mask, byte index) {
        String maskStr = byteToString(mask);
        String indexStr = byteToString(index);

        StringBuilder sb = new StringBuilder(8);
        for (int i = 0; i < 8; i++) {
            if (maskStr.charAt(i) == '1') {
                sb.append(indexStr.charAt(i));
            } else {
                sb.append('*');
            }
        }
        return sb.toString();
    }

    public static boolean testIndex(byte indexValue, byte indexMask, byte indexFilter) {
        return (indexValue & indexMask) == indexFilter;
    }

    /**
     * Check if an index can be used given an Operator and a Value.
     *
     * <code>
     *     -----------===========    --> Index covering
     *     0000000000011111111111    --> Index values
     *
     *     Operator "="
     *     -----------xxxxxQxxxxx    --> Positive index covers partially
     *     xxxxxQxxxxx-----------    --> Negated index covers partially
     *
     *     Operator "<"
     *     QQQQQQxxxxx-----------    --> Negated index covers partially.
     *     QQQQQQQQQQQ######-----    --> Can not use index!
     *
     *     Operator ">"
     *     -----------xxxQQQQQQQQ    --> Positive index covers partially.
     *     -----######QQQQQQQQQQQ    --> Can not use index!
     *
     *     = : Values where the index filter is true
     *     Q : Requested values
     *     x : Extra returned values
     *     # : Requested but missing values. Invalid index if any
     * </code>
     *
     *
     * @param op                Query operator
     * @param value             Query value
     * @param indexThreshold    Index threshold
     * @return                  Null if the index can not be applied. True or False to indicate how to use the index.
     */
    public static Boolean intersectIndexGreaterThan(String op, double value, double indexThreshold) {
        switch (op) {
            case "":
            case "=":
            case "==":
                if (value > indexThreshold) {
                    return Boolean.TRUE;
                } else {
                    return Boolean.FALSE;
                }
            case "<=":
            case "<<=":
                // No need to add DELTA to value, as the index negated is already covering the "<=" operator
            case "<":
            case "<<":
                if (value <= indexThreshold) {
                    return Boolean.FALSE;
                } else {
                    return null;
                }
            case ">=":
            case ">>=":
                // Index is using operator ">". To use ">=" operator, need to subtract a DELTA to the value
                value -= DELTA;
            case ">":
            case ">>":
                if (value >= indexThreshold) {
                    return Boolean.TRUE;
                } else {
                    return null;
                }
            default:
                throw new VariantQueryException("Unknown query operator" + op);
        }
    }
}
