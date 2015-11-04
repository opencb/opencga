package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.models.variant.VariantSource;

/**
 * Created on 26/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStorageTest {

    VariantStorageManager getVariantStorageManager() throws Exception;

    void clearDB(String dbName) throws Exception;

    int getExpectedNumLoadedVariants(VariantSource source) throws Exception;
}
