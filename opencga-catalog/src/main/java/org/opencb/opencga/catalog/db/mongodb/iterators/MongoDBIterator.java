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

package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.api.DBIterator;

import java.util.*;
import java.util.function.Function;

/**
 * Created by imedina on 27/01/16.
 */
public class MongoDBIterator<E> implements DBIterator<E> {

    protected MongoCursor mongoCursor;
    protected GenericDocumentComplexConverter<E> converter;
    protected Function<Document, Document> filter;

    private static final String SEPARATOR = "__";

    public MongoDBIterator(MongoCursor mongoCursor) { //Package protected
        this(mongoCursor, null, null);
    }

    public MongoDBIterator(MongoCursor mongoCursor, GenericDocumentComplexConverter<E> converter) { //Package protected
        this(mongoCursor, converter, null);
    }

    public MongoDBIterator(MongoCursor mongoCursor, Function<Document, Document> filter) { //Package protected
        this(mongoCursor, null, filter);
    }

    public MongoDBIterator(MongoCursor mongoCursor, GenericDocumentComplexConverter<E> converter, Function<Document, Document> filter) {
        //Package protected
        this.mongoCursor = mongoCursor;
        this.converter = converter;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        return mongoCursor.hasNext();
    }

    @Override
    public E next() {
        Document next = (Document) mongoCursor.next();

        if (filter != null) {
            next = filter.apply(next);
        }

        if (converter != null) {
            return converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }

    @Override
    public void close() {
        mongoCursor.close();
    }

    protected void addAclInformation(Document document, QueryOptions options) {
        if (document == null) {
            return;
        }

        if (!options.getBoolean(INCLUDE_ACLS)) {
            return;
        }

        // We have to include the acls to the current document
        List<String> aclList = (List<String>) document.get("_acl");
        if (aclList == null || aclList.isEmpty()) {
            // No acls present in the current document
            return;
        }

        // We will return the acls in a map <member (user or group id), Set<permissions>>
        Map<String, Set<String>> permissionMap = new HashMap<>();
        aclList.forEach(permission -> {
            String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(permission, SEPARATOR, 2);

            if (!"NONE".equals(split[1])) {
                if (!permissionMap.containsKey(split[0])) {
                    Set<String> permissions = new HashSet<>();
                    permissionMap.put(split[0], permissions);
                }
                Set<String> permissions = permissionMap.get(split[0]);
                permissions.add(split[1]);
            }
        });

        // We store those acls retrieved in the attributes field
        Document attributes = (Document) document.get("attributes");
        if (attributes == null) {
            attributes = new Document();
            document.put("attributes", attributes);
        }
        attributes.put("acl", permissionMap);
    }

}
