package org.opencb.opencga.storage.mongodb.variant.search;

import org.bson.conversions.Bson;

import static com.mongodb.client.model.Updates.set;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.INDEX_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.INDEX_TIMESTAMP_FIELD;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantSearchIndexUtils {

//    private static final Bson SET_INDEX_NOT_SYNCHRONIZED =
//            set(INDEX_FIELD + '.' + INDEX_SYNCHRONIZED_FIELD, singletonList(SyncStatus.NOT_SYNCHRONIZED.key()));
//
//    private static final Bson SET_INDEX_SYNCHRONIZED =
//            set(INDEX_FIELD + '.' + INDEX_SYNCHRONIZED_FIELD, singletonList(SyncStatus.SYNCHRONIZED.key()));
//
//    private static final Bson SET_INDEX_UNKNOWN =
//            addToSet(INDEX_FIELD + '.' + INDEX_SYNCHRONIZED_FIELD, SyncStatus.UNKNOWN.key());

    public static Bson getSetIndexNotSynchronized(long value) {
        return set(INDEX_FIELD + '.' + INDEX_TIMESTAMP_FIELD, value);
    }

//    public static Bson getSetIndexSynchronized() {
//        return SET_INDEX_SYNCHRONIZED;
//    }

    public static Bson getSetIndexUnknown(long ts) {
        return set(INDEX_FIELD + '.' + INDEX_TIMESTAMP_FIELD, ts);
    }
}
