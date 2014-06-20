package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantConverter implements ComplexTypeConverter<DBObject, Variant> {

    @Override
    public Variant convert(DBObject object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
