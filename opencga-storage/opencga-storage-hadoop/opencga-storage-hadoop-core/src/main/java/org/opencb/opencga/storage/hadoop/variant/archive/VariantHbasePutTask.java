/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantHbasePutTask implements DataWriter<VcfSlice> {
    protected final Logger logger = LoggerFactory.getLogger(VariantHbasePutTask.class);
    private final ArchiveTableHelper helper;
    private final TableName tableName;
    private final HBaseManager hBaseManager;
    private boolean closeHBaseManager;
    private BufferedMutator tableMutator;

    public VariantHbasePutTask(ArchiveTableHelper helper, String tableName) {
        this(helper, tableName, null);
    }

    public VariantHbasePutTask(ArchiveTableHelper helper, String tableName, HBaseManager hBaseManager) {
        this.helper = helper;
        this.tableName = TableName.valueOf(tableName);
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(helper.getConf());
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
    }

    private ArchiveTableHelper getHelper() {
        return helper;
    }

    @Override
    public boolean open() {
        try {
            logger.info("Open connection using " + getHelper().getConf());
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
                Put put = getHelper().wrap(slice);
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
