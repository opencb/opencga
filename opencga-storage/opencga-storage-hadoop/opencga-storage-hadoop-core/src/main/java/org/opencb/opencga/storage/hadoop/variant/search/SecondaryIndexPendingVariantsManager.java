package org.opencb.opencga.storage.hadoop.variant.search;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsReader;

public class SecondaryIndexPendingVariantsManager extends PendingVariantsManager {
    private final VariantHadoopDBAdaptor dbAdaptor;

    public SecondaryIndexPendingVariantsManager(VariantHadoopDBAdaptor dbAdaptor) {
        super(dbAdaptor, new SecondaryIndexPendingVariantsDescriptor());
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    public PendingVariantsReader reader(Query query) {
        return new SecondaryIndexPendingVariantsReader(query, dbAdaptor);
    }
}
