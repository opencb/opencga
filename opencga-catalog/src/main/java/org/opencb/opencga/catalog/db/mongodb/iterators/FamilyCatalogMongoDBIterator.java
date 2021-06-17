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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FamilyMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.IndividualMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Annotable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class FamilyCatalogMongoDBIterator<E> extends AnnotableCatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private IndividualMongoDBAdaptor individualDBAdaptor;
    private QueryOptions individualQueryOptions;

    private Queue<Document> familyListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;

    public FamilyCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                        AnnotableConverter<? extends Annotable> converter, Function<Document, Document> filter,
                                        IndividualMongoDBAdaptor individualDBAdaptor, QueryOptions options) {
        this(mongoCursor, clientSession, converter, filter, individualDBAdaptor, 0, null, options);
    }


    public FamilyCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                        AnnotableConverter<? extends Annotable> converter, Function<Document, Document> filter,
                                        IndividualMongoDBAdaptor individualDBAdaptor, long studyUid, String user, QueryOptions options) {
        super(mongoCursor, clientSession, converter, filter, options);

        this.user = user;
        this.studyUid = studyUid;

        this.individualDBAdaptor = individualDBAdaptor;
        this.individualQueryOptions = createIndividualQueryOptions();

        this.familyListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(FamilyCatalogMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = familyListBuffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

        addAclInformation(next, options);

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }


    @Override
    public boolean hasNext() {
        if (familyListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !familyListBuffer.isEmpty();
    }

    private void fetchNextBatch() {
        Set<String> memberVersions = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document familyDocument = mongoCursor.next();

            if (user != null && studyUid <= 0) {
                studyUid = ((Number) familyDocument.get(PRIVATE_STUDY_UID)).longValue();
            }

            familyListBuffer.add(familyDocument);
            counter++;

            // Extract all the members
            Object members = familyDocument.get(FamilyMongoDBAdaptor.QueryParams.MEMBERS.key());
            if (members != null && !options.getBoolean(NATIVE_QUERY)) {
                List<Document> memberList = (List<Document>) members;
                if (!memberList.isEmpty()) {
                    memberList.forEach(s -> {
                        String uid = String.valueOf(s.get(FamilyDBAdaptor.QueryParams.UID.key()));
                        String version = String.valueOf(s.get(FamilyDBAdaptor.QueryParams.VERSION.key()));

                        memberVersions.add(uid + "__" + version);
                    });
                }
            }
        }

        if (!memberVersions.isEmpty()) {
            // Obtain all those members

            List<Long> uidList = new ArrayList<>(memberVersions.size());
            List<Integer> versionList = new ArrayList<>(memberVersions.size());
            memberVersions.forEach(s -> {
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "__");
                uidList.add(Long.valueOf(split[0]));
                versionList.add(Integer.valueOf(split[1]));
            });

            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.UID.key(), uidList)
                    .append(IndividualDBAdaptor.QueryParams.VERSION.key(), versionList);
            List<Document> memberList;
            try {
                if (user != null) {
                    query.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                    memberList = individualDBAdaptor.nativeGet(clientSession, studyUid, query, individualQueryOptions, user).getResults();
                } else {
                    memberList = individualDBAdaptor.nativeGet(clientSession, query, individualQueryOptions).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the members associated to the families: {}", e.getMessage(), e);
                return;
            }

            // Map each member uid - version to the member entry
            Map<String, Document> memberUidVersionMap = new HashMap<>(memberList.size());
            // Map each member uid to the member entry
            Map<String, Document> memberUidMap = new HashMap<>(memberList.size());
            memberList.forEach(member -> {
                        memberUidVersionMap.put(member.get(FamilyDBAdaptor.QueryParams.UID.key()) + "__"
                                + member.get(FamilyDBAdaptor.QueryParams.VERSION.key()), member);
                        memberUidMap.put(String.valueOf(member.get(FamilyDBAdaptor.QueryParams.UID.key())), member);
                    }
            );

            // Add the members obtained to the corresponding families
            familyListBuffer.forEach(family -> {
                List<Document> tmpMemberList = new ArrayList<>();
                List<Document> members = (List<Document>) family.get(FamilyMongoDBAdaptor.QueryParams.MEMBERS.key());

                members.forEach(s -> {
                    String uid = String.valueOf(s.get(FamilyDBAdaptor.QueryParams.UID.key()));
                    String version = String.valueOf(s.get(FamilyDBAdaptor.QueryParams.VERSION.key()));
                    String key = uid + "__" + version;

                    // If the members has been returned... (it might have not been fetched due to permissions issues)
                    if (memberUidVersionMap.containsKey(key)) {
                        tmpMemberList.add(memberUidVersionMap.get(key));

                        // We try to fill the parent id with the information we have
                        fillParentInformation(memberUidVersionMap.get(key), IndividualDBAdaptor.QueryParams.FATHER.key(), memberUidMap);
                        fillParentInformation(memberUidVersionMap.get(key), IndividualDBAdaptor.QueryParams.MOTHER.key(), memberUidMap);
                    }
                });

                family.put(FamilyMongoDBAdaptor.QueryParams.MEMBERS.key(), tmpMemberList);
            });
        }
    }

    private void fillParentInformation(Document member, String key, Map<String, Document> memberUidMap) {
        Document parent = (Document) member.get(key);
        if (parent != null && !parent.isEmpty()) {
            Object uidObject = parent.get(IndividualDBAdaptor.QueryParams.UID.key());
            if (uidObject instanceof Long) {
                Document parentDocument = memberUidMap.get(String.valueOf((long) uidObject));
                if (parentDocument != null) {
                    parent.put(IndividualDBAdaptor.QueryParams.ID.key(), parentDocument.get(IndividualDBAdaptor.QueryParams.ID.key()));
                    parent.put(IndividualDBAdaptor.QueryParams.UUID.key(), parentDocument.get(IndividualDBAdaptor.QueryParams.UUID.key()));
                }
            }

        }
    }

    private QueryOptions createIndividualQueryOptions() {
        QueryOptions queryOptions = new QueryOptions();

        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            List<String> includeList = new ArrayList<>();
            for (String include : currentIncludeList) {
                if (include.startsWith(FamilyDBAdaptor.QueryParams.MEMBERS.key() + ".")) {
                    includeList.add(include.replace(FamilyDBAdaptor.QueryParams.MEMBERS.key() + ".", ""));
                }
            }
            if (!includeList.isEmpty()) {
                // If we only have include uid or version, there is no need for an additional query so we will set current options to
                // native query
                boolean includeAdditionalFields = includeList.stream().anyMatch(
                        field -> !field.equals(IndividualDBAdaptor.QueryParams.VERSION.key())
                                && !field.equals(IndividualDBAdaptor.QueryParams.UID.key())
                );
                if (includeAdditionalFields) {
                    includeList.add(IndividualDBAdaptor.QueryParams.VERSION.key());
                    includeList.add(IndividualDBAdaptor.QueryParams.UID.key());
                    queryOptions.put(QueryOptions.INCLUDE, includeList);
                } else {
                    // User wants to include fields already retrieved
                    options.put(NATIVE_QUERY, true);
                }
            }
        }
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> currentExcludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            List<String> excludeList = new ArrayList<>();
            for (String exclude : currentExcludeList) {
                if (exclude.startsWith(FamilyDBAdaptor.QueryParams.MEMBERS.key() + ".")) {
                    String replace = exclude.replace(FamilyDBAdaptor.QueryParams.MEMBERS.key() + ".", "");
                    if (!FamilyDBAdaptor.QueryParams.UID.key().equals(replace)
                            && !FamilyDBAdaptor.QueryParams.VERSION.key().equals(replace)) {
                        excludeList.add(replace);
                    }
                }
            }
            if (!excludeList.isEmpty()) {
                queryOptions.put(QueryOptions.EXCLUDE, excludeList);
            } else {
                queryOptions.remove(QueryOptions.EXCLUDE);
            }
        }
        if (options.containsKey(ParamConstants.FLATTEN_ANNOTATIONS)) {
            queryOptions.put(ParamConstants.FLATTEN_ANNOTATIONS, options.getBoolean(ParamConstants.FLATTEN_ANNOTATIONS));
        }

        return queryOptions;
    }


}
