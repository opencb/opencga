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

package org.opencb.opencga.storage.mongodb.variant.adaptors;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoPersistentCursor;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jacobo on 9/01/15.
 */
public class VariantMongoDBIterator extends VariantDBIterator {

    private MongoCursor<Document> dbCursor;
    private DocumentToVariantConverter documentToVariantConverter;
    private int count;
    private Logger logger = LoggerFactory.getLogger(VariantMongoDBIterator.class);

    //Package protected
    VariantMongoDBIterator(FindIterable<Document> dbCursor, DocumentToVariantConverter documentToVariantConverter) {
        this(dbCursor, documentToVariantConverter, 100);
    }

    //Package protected
    VariantMongoDBIterator(FindIterable<Document> dbCursor, DocumentToVariantConverter documentToVariantConverter, int batchSize) {
        this.documentToVariantConverter = documentToVariantConverter;
        if (batchSize > 0) {
            dbCursor.batchSize(batchSize);
        }
        this.dbCursor = fetch(dbCursor::iterator);
    }

    //Package protected
    static VariantMongoDBIterator persistentIterator(MongoDBCollection collection, Bson query, Bson projection, QueryOptions options,
                                                     DocumentToVariantConverter converter) {
        StopWatch watch = StopWatch.createStarted();
        MongoPersistentCursor cursor = new MongoPersistentCursor(collection, query, projection, options);
        VariantMongoDBIterator iterator = new VariantMongoDBIterator(cursor, converter);
        iterator.timeFetching += watch.getNanoTime();
        return iterator;
    }

    //Package protected
    VariantMongoDBIterator(MongoCursor<Document> cursor,
                           DocumentToVariantConverter documentToVariantConverter) {
        this.documentToVariantConverter = documentToVariantConverter;
        this.dbCursor = cursor;
    }

    @Override
    public boolean hasNext() {
        return fetch(() -> dbCursor.hasNext());
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public Variant next() {
        Document document = fetch(() -> dbCursor.next());
        try {
            count++;
            return convert(() -> documentToVariantConverter.convertToDataModelType(document));
        } catch (RuntimeException e) {
            logger.error("Error converting variant " + document.getString("_id"));
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        dbCursor.close();
    }
}
