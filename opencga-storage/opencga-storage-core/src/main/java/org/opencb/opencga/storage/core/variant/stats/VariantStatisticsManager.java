package org.opencb.opencga.storage.core.variant.stats;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;
import java.util.List;

/**
 * Created on 02/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStatisticsManager {

    /**
     *
     * @param study     Study
     * @param cohorts   Cohorts to calculate stats
     * @param options   Other options
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageManager.Options#AGGREGATION_MAPPING_PROPERTIES}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageManager.Options#OVERWRITE_STATS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageManager.Options#UPDATE_STATS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageManager.Options#LOAD_THREADS}
     *                  {@link org.opencb.opencga.storage.core.variant.VariantStorageManager.Options#LOAD_BATCH_SIZE}
     *                  {@link org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams#REGION}
     *
     * @throws StorageManagerException      If there is any problem related with the StorageManager
     * @throws IOException                  If there is any IO problem
     */
    void calculateStatistics(String study, List<String> cohorts, QueryOptions options) throws IOException, StorageManagerException;

}
