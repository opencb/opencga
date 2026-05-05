package org.opencb.opencga.storage.mongodb.variant.search;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Updates.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.INDEX_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.INDEX_TIMESTAMP_FIELD;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantSearchIndexUtils {

    /**
     * List of study IDs that were indexed in the last search index run.
     */
    public static final String INDEX_STUDIES_FIELD = "st";

    /**
     * Stats hash string from the last search index run (format: "key=value,key=value").
     */
    public static final String INDEX_STATS_HASH_FIELD = "sh";

    public static Bson getSetIndexNotSynchronized(long value) {
        return set(INDEX_FIELD, new Document(INDEX_TIMESTAMP_FIELD, value));
    }

    public static Bson getSetIndexStatsNotSynchronized(long value) {
        return set(INDEX_FIELD + '.' + INDEX_TIMESTAMP_FIELD, value);
    }

    public static Bson getSetIndexUnknown(long ts) {
        return set(INDEX_FIELD, new Document(INDEX_TIMESTAMP_FIELD, ts));
    }

    public static Bson getSetSyncStatus(Set<Integer> studies, Map<Integer, Long> statsHash) {
        List<Bson> updates = new ArrayList<>();
        updates.add(set(INDEX_FIELD + '.' + INDEX_STUDIES_FIELD, new ArrayList<>(studies)));
        if (statsHash != null && !statsHash.isEmpty()) {
            String sh = statsHash.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(","));
            updates.add(set(INDEX_FIELD + '.' + INDEX_STATS_HASH_FIELD, sh));
        } else {
            updates.add(unset(INDEX_FIELD + '.' + INDEX_STATS_HASH_FIELD));
        }
        return combine(updates);
    }
}
