/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public void pre() throws Exception {
        super.pre();
        options.put(INDEXES_CREATED, false);
    }

    @Override
    public synchronized void post() throws Exception {
        super.post();
        if (!options.getBoolean(INDEXES_CREATED)) {
            dbAdaptor.createIndexes(new QueryOptions());
            options.put(INDEXES_CREATED, true);
        }
    }
}
