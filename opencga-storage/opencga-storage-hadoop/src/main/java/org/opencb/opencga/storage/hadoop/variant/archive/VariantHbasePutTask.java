package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.commons.io.DataWriter;
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
    private final ArchiveHelper helper;
    private final TableName tableName;
    private BufferedMutator tableMutator;

    public VariantHbasePutTask(ArchiveHelper helper, String tableName) {
        this.helper = helper;
        this.tableName = TableName.valueOf(tableName);
    }

    private ArchiveHelper getHelper() {
        return helper;
    }

    @Override
    public boolean open() {
        try {
            logger.info("Open connection using " + getHelper().getConf());
            tableMutator = getHelper().getHBaseManager().getConnection().getBufferedMutator(this.tableName);
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
            getHelper().close();
        } catch (Exception e) {
            throw new IllegalStateException("Problems closing connection", e);
        }
        return true;
    }
}
