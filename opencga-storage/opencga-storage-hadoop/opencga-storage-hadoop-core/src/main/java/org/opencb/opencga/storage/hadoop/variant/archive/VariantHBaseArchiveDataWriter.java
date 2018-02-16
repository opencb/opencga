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

package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantToVcfSliceConverterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantHBaseArchiveDataWriter extends AbstractHBaseDataWriter<VcfSlice, Put> {
    protected final Logger logger = LoggerFactory.getLogger(VariantHBaseArchiveDataWriter.class);
    private final ArchiveTableHelper helper;

    public VariantHBaseArchiveDataWriter(ArchiveTableHelper helper, String tableName, HBaseManager hBaseManager) {
        super(hBaseManager, tableName);
        this.helper = helper;
    }

    @Override
    protected List<Put> convert(List<VcfSlice> batch) {
        if (batch.isEmpty()) {
            return Collections.emptyList();
        }
        List<Put> putLst = new ArrayList<>(batch.size());
        for (VcfSlice slice : batch) {
            // TODO: Modify input to have slices already sorted
            Put put = helper.wrap(slice, isRefSlice(slice));

            putLst.add(put);
        }
        return putLst;
    }

    public static boolean isRefSlice(VcfSlice slice) {
        for (String gt : slice.getFields().getGtsList()) {
            if (!VariantToVcfSliceConverterTask.isHomRef(gt)) {
                return false;
            }
        }
        return true;
    }

}
