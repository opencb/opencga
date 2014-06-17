package org.opencb.opencga.storage.variant.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class HBaseToArchivedVariantFileConverter implements ComplexTypeConverter<ArchivedVariantFile, Result> {

    @Override
    public ArchivedVariantFile convertToDataModelType(Result object) {
        // TODO Implementation pending
        return null;
    }

    @Override
    public Result convertToStorageType(ArchivedVariantFile object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
