package org.opencb.opencga.storage.variant.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class ArchivedVariantFileToHbaseConverter implements ComplexTypeConverter<ArchivedVariantFile, Put> {

    @Override
    public ArchivedVariantFile convertToDataModelType(Put object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Put convertToStorageType(ArchivedVariantFile object) {
        // TODO Implementation pending
        return null;
    }
    
    
}
