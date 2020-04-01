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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by imedina on 27/01/16.
 */
public class CatalogMongoDBIterator<E> implements DBIterator<E> {

    protected MongoDBIterator<Document> mongoCursor;
    protected ClientSession clientSession;
    protected GenericDocumentComplexConverter<E> converter;
    protected Function<Document, Document> filter;

    protected static final String PRIVATE_STUDY_UID = MongoDBAdaptor.PRIVATE_STUDY_UID;

    private static final String SEPARATOR = "__";

    public CatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor) { //Package protected
        this(mongoCursor, null, null, null);
    }

    public CatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, GenericDocumentComplexConverter<E> converter) { //Package protected
        this(mongoCursor, null, converter, null);
    }

    public CatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                  GenericDocumentComplexConverter<E> converter, Function<Document, Document> filter) {
        //Package protected
        this.mongoCursor = mongoCursor;
        this.clientSession = clientSession;
        this.converter = converter;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        return mongoCursor.hasNext();
    }

    @Override
    public E next() {
        Document next = mongoCursor.next();

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

    @Override
    public long getNumMatches() {
        return mongoCursor.getNumMatches();
    }

    void addAclInformation(Document document, QueryOptions options) {
        if (document == null) {
            return;
        }

        if (!options.getBoolean(INCLUDE_ACLS)) {
            return;
        }

        // We have to include the acls to the current document
        List<String> aclList = (List<String>) document.get("_acl");
        List<Document> permissions = new ArrayList<>();
        if (aclList != null && !aclList.isEmpty()) {
            // We will return the acls following the AclEntry format
            Map<String, List<String>> permissionMap = new HashMap<>();
            aclList.forEach(permission -> {
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(permission, SEPARATOR, 2);
                if (!permissionMap.containsKey(split[0])) {
                    List<String> tmpPermissions = new ArrayList<>();
                    permissionMap.put(split[0], tmpPermissions);
                }
                if (!"NONE".equals(split[1])) {
                    List<String> tmpPermissions = permissionMap.get(split[0]);
                    tmpPermissions.add(split[1]);
                }
            });
            // We parse the map to the AclEntry format
            permissionMap.entrySet().forEach(entry -> permissions.add(new Document()
                    .append("member", entry.getKey())
                    .append("permissions", entry.getValue()))
            );
        }

        // We store those acls retrieved in the attributes field
        Document attributes = (Document) document.get("attributes");
        if (attributes == null) {
            attributes = new Document();
            document.put("attributes", attributes);
        }
        attributes.put("OPENCGA_ACL", permissions);
    }

}
