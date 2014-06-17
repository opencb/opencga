package org.opencb.opencga.storage.variant.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantStatsToHbaseConverter implements ComplexTypeConverter<VariantStats, Put> {

    @Override
    public VariantStats convertToDataModelType(Put object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Put convertToStorageType(VariantStats object) {
        // TODO Implementation pending
        return null;
    }
    
}
