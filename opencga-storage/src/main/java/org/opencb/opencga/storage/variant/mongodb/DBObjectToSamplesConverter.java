package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public abstract class DBObjectToSamplesConverter implements ComplexTypeConverter<ArchivedVariantFile, DBObject> {

}
