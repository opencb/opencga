package org.opencb.opencga.storage.mongodb.variant.io.db;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.io.db.VariantAnnotationDBWriter;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;

/**
 * Basic functionality of VariantAnnotationDBWriter. Creates MongoDB indexes at the post step (if needed).
 * Created on 05/01/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBAnnotationDBWriter extends VariantAnnotationDBWriter {
    private static final String INDEXES_CREATED = "indexes.created";
    private final VariantMongoDBAdaptor dbAdaptor;

    public VariantMongoDBAnnotationDBWriter(QueryOptions options, VariantMongoDBAdaptor dbAdaptor) {
        super(dbAdaptor, options, null);
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    public void pre() {
        super.pre();
        options.put(INDEXES_CREATED, false);
    }

    @Override
    public synchronized void post() {
        super.post();
        if (!options.getBoolean(INDEXES_CREATED)) {
            dbAdaptor.createIndexes(new QueryOptions());
            options.put(INDEXES_CREATED, true);
        }
    }
}
