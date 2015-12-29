/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;

import java.util.Comparator;

/**
 * Compares two VcfRecords and sorts them by.
 * <ul>
 * <li>first {@link VcfRecord#getRelativeStart()}
 * <li>first {@link VcfRecord#getRelativeEnd()()}
 * <li>string compare {@link VcfRecord#getAlternateList()}
 * </ul>
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VcfRecordComparator implements Comparator<VcfRecord> {

    @Override
    public int compare(VcfRecord o1, VcfRecord o2) {
        int compare = Integer.compare(o1.getRelativeStart(), o2.getRelativeStart());
        if (compare != 0) {
            return compare;
        }

        compare = Integer.compare(o1.getRelativeEnd(), o2.getRelativeEnd());
        if (compare != 0) {
            return compare;
        }

        compare = StringUtils.join(o1.getAlternateList(), ";").compareTo(StringUtils.join(o2.getAlternateList(), ";"));
        if (compare != 0) {
            return compare;
        }

        byte[] a = o1.toByteArray(); // should not reach this point often
        byte[] b = o2.toByteArray();
        compare = Integer.compare(a.length, b.length);
        if (compare != 0) {
            return compare;
        }

        return Bytes.compareTo(a, b);
    }

}
