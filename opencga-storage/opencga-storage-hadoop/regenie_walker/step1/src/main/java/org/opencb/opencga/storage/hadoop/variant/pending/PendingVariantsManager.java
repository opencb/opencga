package org.opencb.opencga.storage.hadoop.variant.pending;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;

public abstract class PendingVariantsManager {

    private final PendingVariantsDescriptor descriptor;
    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;

    protected PendingVariantsManager(VariantHadoopDBAdaptor dbAdaptor, PendingVariantsDescriptor descriptor) {
        hBaseManager = dbAdaptor.getHBaseManager();
        tableNameGenerator = dbAdaptor.getTableNameGenerator();
        this.descriptor = descriptor;
    }

    protected PendingVariantsManager(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                  PendingVariantsDescriptor descriptor) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.descriptor = descriptor;
    }

    public PendingVariantsReader reader(Query query) {
        return new PendingVariantsReader(query, descriptor, hBaseManager, tableNameGenerator);
    }

    public VariantDBIterator iterator(Query query) {
        return VariantDBIterator.wrapper(reader(query).iterator());
    }

    public PendingVariantsDBCleaner cleaner() {
        return new PendingVariantsDBCleaner(hBaseManager, descriptor.getTableName(tableNameGenerator), descriptor);
    }

    public void discoverPending(MRExecutor mrExecutor, boolean overwrite, ObjectMap options) throws StorageEngineException {
        discoverPending(mrExecutor, new QueryOptions(options).append(DiscoverPendingVariantsDriver.OVERWRITE, overwrite));
    }

    public void discoverPending(MRExecutor mrExecutor, ObjectMap options) throws StorageEngineException {
        mrExecutor.run(DiscoverPendingVariantsDriver.class,
                DiscoverPendingVariantsDriver.buildArgs(
                        tableNameGenerator.getVariantTableName(), descriptor.getClass(), options),
                "Discover pending " + descriptor.name() + " variants");
    }

    public boolean exists() throws StorageEngineException {
        String tableName = descriptor.getTableName(tableNameGenerator);
        try {
            return hBaseManager.tableExists(tableName);
        } catch (IOException e) {
            throw new StorageEngineException("Unable to determine if table '" + tableName + "' exists", e);
        }
    }

    public void deleteTable() throws StorageEngineException {
        String tableName = descriptor.getTableName(tableNameGenerator);
        try {
            if (hBaseManager.tableExists(tableName)) {
                hBaseManager.act(tableName, (table, admin) -> {
                    admin.disableTable(table.getName());
                    admin.deleteTable(table.getName());
                    return null;
                });
            }
        } catch (IOException e) {
            throw new StorageEngineException("Unable to delete table '" + tableName + "'", e);
        }
    }

}
