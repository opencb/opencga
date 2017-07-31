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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantMergerTableMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantHBaseArchiveDataWriter implements DataWriter<VcfSlice> {
    protected final Logger logger = LoggerFactory.getLogger(VariantHBaseArchiveDataWriter.class);
    private final ArchiveTableHelper helper;
    private final TableName tableName;
    private final HBaseManager hBaseManager;
    private final boolean writeVariantsColumn;
    private BufferedMutator tableMutator;

    public VariantHBaseArchiveDataWriter(ArchiveTableHelper helper, String tableName, HBaseManager hBaseManager) {
        this(helper, tableName, hBaseManager, false);
    }

    public VariantHBaseArchiveDataWriter(ArchiveTableHelper helper, String tableName, HBaseManager hBaseManager,
                                         boolean writeVariantsColumn) {
        this.helper = helper;
        this.tableName = TableName.valueOf(tableName);
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(helper.getConf());
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
        this.writeVariantsColumn = writeVariantsColumn;
    }

    @Override
    public boolean open() {
        try {
            logger.info("Open connection using " + helper.getConf());
            tableMutator = hBaseManager.getConnection().getBufferedMutator(this.tableName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to Hbase", e);
        }
        return true;
    }

    @Override
    public boolean write(List<VcfSlice> batch) {
        if (batch.isEmpty()) {
            return true;
        }
        // logger.info("Open to table " + this.tableName.getNameAsString());
        try {
            List<Put> putLst = new ArrayList<>(batch.size());
            for (VcfSlice slice : batch) {
                Put put = helper.wrap(slice);
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
            tableMutator.mutate(putLst);
            return true;
        } catch (IOException e) {
            throw new RuntimeException(String.format("Problems submitting %s data to hbase %s ", batch.size(),
                    this.tableName.getNameAsString()), e);
        }
    }

    @Override
    public boolean close() {
        if (null != tableMutator) {
            try {
                tableMutator.close();
            } catch (IOException e) {
                logger.error("Error closing table mutator", e);
            } finally {
                tableMutator = null;
            }
        }
        try {
            hBaseManager.close();
        } catch (Exception e) {
            throw new IllegalStateException("Problems closing connection", e);
        }
        return true;
    }
}
