package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.ArchivedVariantFile;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToSamplesDecompressedConverter extends DBObjectToSamplesConverter {

    @Override
    public ArchivedVariantFile convertToDataModelType(DBObject object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DBObject convertToStorageType(ArchivedVariantFile object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
