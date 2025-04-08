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

/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;

import java.util.Comparator;

/**
 * Compares two VcfRecords and sorts them by.
 * <ul>
 * <li>first {@link VcfRecord#getRelativeStart()}
 * <li>first {@link VcfRecord#getRelativeEnd()()}
 * <li>string compare {@link VcfRecord#getAlternate()}
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

        compare = o1.getAlternate().compareTo(o2.getAlternate());
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
