package org.opencb.opencga.storage.hbase.variant;

import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class HBaseToVariantConverter implements ComplexTypeConverter<Variant, Result> {

    @Override
    public Variant convertToDataModelType(Result object) {
        // TODO Implementation pending
        return null;
    }

    @Override
    public Result convertToStorageType(Variant object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
