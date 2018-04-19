package org.opencb.opencga.storage.mongodb.variant.search;

import org.bson.conversions.Bson;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager.SyncStatus;

import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.set;
import static java.util.Collections.singletonList;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.INDEX_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.INDEX_SYNCHRONIZED_FIELD;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantSearchIndexUtils {

    public static final Bson SET_INDEX_NOT_SYNCHRONIZED =
            set(INDEX_FIELD + '.' + INDEX_SYNCHRONIZED_FIELD, singletonList(SyncStatus.NOT_SYNCHRONIZED.key()));

    public static final Bson SET_INDEX_SYNCHRONIZED =
            set(INDEX_FIELD + '.' + INDEX_SYNCHRONIZED_FIELD, singletonList(SyncStatus.SYNCHRONIZED.key()));

    public static final Bson SET_INDEX_UNKNOWN =
            addToSet(INDEX_FIELD + '.' + INDEX_SYNCHRONIZED_FIELD, SyncStatus.UNKNOWN.key());
}
