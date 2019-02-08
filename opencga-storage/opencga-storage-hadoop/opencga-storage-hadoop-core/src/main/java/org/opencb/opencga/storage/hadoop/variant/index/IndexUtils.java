package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;

/**
 * Created on 01/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IndexUtils {

    public static final byte EMPTY_MASK = 0;

    public static String byteToString(byte b) {
        String str = Integer.toBinaryString(b);
        if (str.length() > 8) {
            str = str.substring(str.length() - 8);
        }
        return StringUtils.leftPad(str, 8, '0');
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
}
