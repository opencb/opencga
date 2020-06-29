package org.opencb.opencga.storage.hadoop.variant.search;

import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

public class SecondaryIndexPendingVariantsManager extends PendingVariantsManager {
    public SecondaryIndexPendingVariantsManager(VariantHadoopDBAdaptor dbAdaptor) {
        super(dbAdaptor, new SecondaryIndexPendingVariantsDescriptor());
    }

    public SecondaryIndexPendingVariantsManager(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator) {
        super(hBaseManager, tableNameGenerator, new SecondaryIndexPendingVariantsDescriptor());
    }
}
