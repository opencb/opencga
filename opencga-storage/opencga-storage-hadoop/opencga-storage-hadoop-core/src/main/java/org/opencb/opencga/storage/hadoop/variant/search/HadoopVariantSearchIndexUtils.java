package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.PBoolean;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;

import java.util.List;

/**
 * Created on 23/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchIndexUtils {

    /**
     * Marks the row as Not Sync Status. This method should be called when loading annotations or statistics, and when removing a study.
     *
     * @param put Mutation to add new Variant information.
     * @param columnFamily Main column family.
     * @return The same put operation with the {@link VariantPhoenixHelper.VariantColumn#INDEX_NOT_SYNC} column.
     */
    public static Put addNotSyncStatus(Put put, byte[] columnFamily) {
        if (put != null) {
            put.addColumn(columnFamily, VariantPhoenixHelper.VariantColumn.INDEX_NOT_SYNC.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
        return put;
    }

    /**
     * Marks the row as Unknown Sync Status. This method should be called when loading or removing files.
     *
     * @param put Mutation to add new Variant information.
     * @param columnFamily Main column family.
     * @return The same put operation with the {@link VariantPhoenixHelper.VariantColumn#INDEX_UNKNOWN} column.
     */
    public static Put addUnknownSyncStatus(Put put, byte[] columnFamily) {
        if (put != null) {
            put.addColumn(columnFamily, VariantPhoenixHelper.VariantColumn.INDEX_UNKNOWN.bytes(), System.currentTimeMillis(),
                    PBoolean.TRUE_BYTES);
        }
        return put;
    }

    public static VariantStorageEngine.SyncStatus getSyncStatus(boolean noSync, boolean unknown, List<Integer> studies) {
        final VariantStorageEngine.SyncStatus syncStatus;
        if (noSync) {
            syncStatus = VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED;
        } else {
            if (unknown) {
                syncStatus = VariantStorageEngine.SyncStatus.UNKNOWN;
            } else {
                if (studies == null || studies.isEmpty()) {
                    syncStatus = VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED;
                } else {
                    syncStatus = VariantStorageEngine.SyncStatus.SYNCHRONIZED;
                }
            }
        }
        return syncStatus;
    }
}
