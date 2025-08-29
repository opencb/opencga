package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.util.function.Function;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface PendingVariantsDescriptor<R> {

    String name();

    enum Type {
        FILE, TABLE
    }

    Type getType();

    /**
     * Configure the scan to read from the variants table.
     *
     * @param scan Scan to configure
     * @param metadataManager Metadata manager
     * @return The same scan object
     */
    Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager);

    Function<Result, R> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite);

}
