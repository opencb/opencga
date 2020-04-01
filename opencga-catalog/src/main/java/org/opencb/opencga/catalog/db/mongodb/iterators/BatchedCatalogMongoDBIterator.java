/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Function;

public abstract class BatchedCatalogMongoDBIterator<T> extends CatalogMongoDBIterator<T> {

    private Queue<Document> buffer = new LinkedList<>();

    protected final QueryOptions options;

    public BatchedCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, GenericDocumentComplexConverter<T> converter) {
        this(mongoCursor, null, converter, null, null);
    }

    public BatchedCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                         GenericDocumentComplexConverter<T> converter, Function<Document, Document> filter,
                                         QueryOptions options) {
        super(mongoCursor, clientSession, converter, filter);
        this.options = options == null ? QueryOptions.empty() : options;
    }


    @Override
    public boolean hasNext() {
        if (buffer.isEmpty()) {
            fetchNextBatch(buffer, 100);
        }
        return !buffer.isEmpty();
    }

    protected abstract void fetchNextBatch(Queue<Document> buffer, int bufferSize);

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Document next = buffer.remove();
        addAclInformation(next, options);

        if (filter != null) {
            next = filter.apply(next);
        }

        return convert(next);
    }

    protected T convert(Document next) {
        if (converter != null) {
            return converter.convertToDataModelType(next);
        } else {
            return (T) next;
        }
    }
}
