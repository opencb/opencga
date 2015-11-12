/**
 * 
 */
package org.opencb.opencga.storage.hadoop.mr;

import java.util.Comparator;

import com.google.protobuf.ProtocolStringList;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;

/**
 * Compares two VcfRecords and sorts them by
 * <ul>
 * <li>first {@link VcfRecord#getRelativeStart()}
 * <li>first {@link VcfRecord#getRelativeEnd()()}
 * <li>string compare {@link VcfRecord#getAlternateList()}
 * <ul>
 * 
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VcfRecordComparator implements Comparator<VcfRecord> {

    @Override
    public int compare (VcfRecord o1, VcfRecord o2) {
        int compare = Integer.compare(o1.getRelativeStart(), o2.getRelativeStart());
        if (compare != 0)
            return compare;
        compare = Integer.compare(o1.getRelativeEnd(), o2.getRelativeEnd());
        if (compare != 0)
            return compare;
        return o1.getAlternateList().equals(o2.getAlternateList()) ? 0 : 1;
    }

}
