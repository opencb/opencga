package org.opencb.opencga.storage.hadoop.variant.search;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
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

    @Override
    public void discoverPending(MRExecutor mrExecutor, ObjectMap options) throws StorageEngineException {
        options = new ObjectMap(options);
        // Never filter by study!
        options.remove(VariantQueryParam.STUDY.key());
        super.discoverPending(mrExecutor, options);
    }
}
