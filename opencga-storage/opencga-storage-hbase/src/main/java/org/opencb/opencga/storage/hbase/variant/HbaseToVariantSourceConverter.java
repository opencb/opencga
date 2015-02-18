package org.opencb.opencga.storage.hbase.variant;

import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class HbaseToVariantSourceConverter implements ComplexTypeConverter<VariantSource, Result> {

    @Override
    public VariantSource convertToDataModelType(Result object) {
        // TODO Implementation pending
        return null;
    }

    @Override
    public Result convertToStorageType(VariantSource object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
