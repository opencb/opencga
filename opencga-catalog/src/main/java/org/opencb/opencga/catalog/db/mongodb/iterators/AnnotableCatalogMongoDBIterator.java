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
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.common.Annotable;

import java.util.function.Function;

public class AnnotableCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    protected QueryOptions options;
    protected AnnotableConverter<? extends Annotable> converter;

    @Deprecated
    public AnnotableCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                           Function<Document, Document> filter, QueryOptions options) {
        super(mongoCursor, null, null, filter);
        this.options = ParamUtils.defaultObject(options, QueryOptions::new);
        this.converter = converter;
    }

    public AnnotableCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                           AnnotableConverter<? extends Annotable> converter, Function<Document, Document> filter,
                                           QueryOptions options) {
        super(mongoCursor, clientSession, null, filter);
        this.options = ParamUtils.defaultObject(options, QueryOptions::new);
        this.converter = converter;
    }

    @Override
    public E next() {
        Document next = mongoCursor.next();

        if (filter != null) {
            next = filter.apply(next);
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }

}
