package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;

/**
 * Created on 01/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IndexUtils {

    public static final byte EMPTY_MASK = 0;

    public static String maskToString(byte b) {
        String str = Integer.toBinaryString(b);
        if (str.length() > 8) {
            str = str.substring(str.length() - 8);
        }
        return StringUtils.leftPad(str, 8, '0');
    }

    public static boolean testIndex(byte indexValue, byte indexMask, byte indexFilter) {
        return (indexValue & indexMask) == indexFilter;
    }
}
