package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantType;

/**
 * Created on 26/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStorageTest {

    VariantStorageManager getVariantStorageManager() throws Exception;

    void clearDB(String dbName) throws Exception;

    default int getExpectedNumLoadedVariants(VariantSource source) throws Exception {
        return source.getStats().getNumRecords();
    }
}
