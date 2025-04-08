package org.opencb.opencga.storage.hadoop.variant.prune;

import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsManager;

public class SecondaryIndexPrunePendingVariantsManager extends PendingVariantsManager {

    public SecondaryIndexPrunePendingVariantsManager(VariantHadoopDBAdaptor dbAdaptor) {
        super(dbAdaptor, new SecondaryIndexPrunePendingVariantsDescriptor());
    }

}
