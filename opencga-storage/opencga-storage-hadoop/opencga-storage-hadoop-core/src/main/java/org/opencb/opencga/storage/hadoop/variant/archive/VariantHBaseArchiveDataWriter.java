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
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantMergerTableMapper;
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
    private final boolean writeVariantsColumn;

    public VariantHBaseArchiveDataWriter(ArchiveTableHelper helper, String tableName, HBaseManager hBaseManager) {
        this(helper, tableName, hBaseManager, false);
    }

    public VariantHBaseArchiveDataWriter(ArchiveTableHelper helper, String tableName, HBaseManager hBaseManager,
                                         boolean writeVariantsColumn) {
        super(hBaseManager, tableName);
        this.helper = helper;
        this.writeVariantsColumn = writeVariantsColumn;
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

            // TODO: REMOVE THIS
            if (writeVariantsColumn) {
                for (int i = 0; i < slice.getRecordsCount(); i++) {
                    VcfSliceProtos.VcfRecord record = slice.getRecords(i);
                    VariantType type = VcfRecordProtoToVariantConverter.getVariantType(record.getType());
                    if (VariantMergerTableMapper.TARGET_VARIANT_TYPE_SET.contains(type)) {
                        int start = VcfRecordProtoToVariantConverter.getStart(record, slice.getPosition());
                        String reference = record.getReference();
                        String alternate = record.getAlternate();
                        byte[] column = Bytes.toBytes(GenomeHelper.getVariantColumn(start, reference, alternate));
                        put.addColumn(helper.getColumnFamily(), column, new byte[0]);
                    }
                }
            }
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
