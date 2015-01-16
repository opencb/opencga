package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

/**
 * Created by jacobo on 9/01/15.
 */
public class VariantMongoDBIterator extends VariantDBIterator {

    private DBCursor dbCursor;
    private DBObjectToVariantConverter dbObjectToVariantConverter;

    VariantMongoDBIterator(DBCursor dbCursor, DBObjectToVariantConverter dbObjectToVariantConverter) { //Package protected
        this(dbCursor, dbObjectToVariantConverter, 100);
    }
    VariantMongoDBIterator(DBCursor dbCursor, DBObjectToVariantConverter dbObjectToVariantConverter, int batchSize) { //Package protected
        this.dbCursor = dbCursor;
        this.dbObjectToVariantConverter = dbObjectToVariantConverter;
        if(batchSize > 0) {
            dbCursor.batchSize(batchSize);
        }
    }

    @Override
    public boolean hasNext() {
        return dbCursor.hasNext();
    }

    @Override
    public Variant next() {
        DBObject dbObject = dbCursor.next();
        return dbObjectToVariantConverter.convertToDataModelType(dbObject);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException( "can't remove from a cursor" );
    }
}
