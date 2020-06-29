package org.opencb.opencga.storage.hadoop.variant.pending;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

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

}
