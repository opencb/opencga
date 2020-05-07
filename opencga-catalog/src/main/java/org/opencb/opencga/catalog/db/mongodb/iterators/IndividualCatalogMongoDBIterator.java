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

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.IndividualMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.common.Annotable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class IndividualCatalogMongoDBIterator<E> extends AnnotableCatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private SampleDBAdaptor sampleDBAdaptor;
    private QueryOptions sampleQueryOptions;

    private IndividualDBAdaptor individualDBAdaptor;

    private Queue<Document> individualListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;

    public IndividualCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                            Function<Document, Document> filter, MongoDBAdaptorFactory dbAdaptorFactory,
                                            QueryOptions options) {
        this(mongoCursor, converter, filter, dbAdaptorFactory, 0, null, options);
    }

    public IndividualCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                            Function<Document, Document> filter, MongoDBAdaptorFactory dbAdaptorFactory,
                                            long studyUid, String user, QueryOptions options) {
        super(mongoCursor, converter, filter, options);

        this.user = user;
        this.studyUid = studyUid;

        this.sampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();
        this.sampleQueryOptions = createSampleQueryOptions();

        this.individualDBAdaptor = dbAdaptorFactory.getCatalogIndividualDBAdaptor();

        this.individualListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(IndividualCatalogMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = individualListBuffer.remove();

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
        if (individualListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !individualListBuffer.isEmpty();
    }

    private void fetchNextBatch() {
        Set<String> sampleVersions = new HashSet<>();
        Map<Long, List<Document>> individualMap = new HashMap<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document individualDocument = (Document) mongoCursor.next();

            if (user != null && studyUid <= 0) {
                studyUid = individualDocument.getLong(PRIVATE_STUDY_UID);
            }

            individualListBuffer.add(individualDocument);
            counter++;

            // Extract all the samples
            Object samples = individualDocument.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
            if (samples != null && !options.getBoolean(NATIVE_QUERY)) {
                List<Document> sampleList = (List<Document>) samples;
                if (!sampleList.isEmpty()) {
                    sampleList.forEach(s -> {
                        String uid = String.valueOf(s.get(IndividualDBAdaptor.QueryParams.UID.key()));
                        String version = String.valueOf(s.get(IndividualDBAdaptor.QueryParams.VERSION.key()));

                        // TODO we store in a Set to try to reduce the number of keys to be queried, maybe this is not needed?
                        sampleVersions.add(uid + "__" + version);
                    });
                }
            }

            if (!options.getBoolean(NATIVE_QUERY)) {
                // Extract father and mother uids
                Document father = (Document) individualDocument.get(IndividualDBAdaptor.QueryParams.FATHER.key());
                addParentToMap(individualMap, father);
                Document mother = (Document) individualDocument.get(IndividualDBAdaptor.QueryParams.MOTHER.key());
                addParentToMap(individualMap, mother);
            }
        }

        if (!individualMap.isEmpty()) {
            // Obtain the parents

            Query query = new Query(SampleDBAdaptor.QueryParams.UID.key(), individualMap.keySet());
            QueryOptions queryOptions = new QueryOptions()
                    .append(NATIVE_QUERY, true)
                    .append(QueryOptions.INCLUDE, Arrays.asList(
                            IndividualDBAdaptor.QueryParams.ID.key(), IndividualDBAdaptor.QueryParams.VERSION.key(),
                            IndividualDBAdaptor.QueryParams.UID.key()));

            try {
                DataResult<Document> individualDataResult;
                if (user != null) {
                    query.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                    individualDataResult = individualDBAdaptor.nativeGet(studyUid, query, queryOptions, user);
                } else {
                    individualDataResult = individualDBAdaptor.nativeGet(query, queryOptions);
                }

                for (Document individual : individualDataResult.getResults()) {
                    List<Document> parentList = individualMap.get(individual.getLong(IndividualDBAdaptor.QueryParams.UID.key()));
                    for (Document parentDocument : parentList) {
                        parentDocument.put(IndividualDBAdaptor.QueryParams.ID.key(),
                                individual.getString(IndividualDBAdaptor.QueryParams.ID.key()));
                        parentDocument.put(IndividualDBAdaptor.QueryParams.VERSION.key(),
                                individual.getInteger(IndividualDBAdaptor.QueryParams.VERSION.key()));
                    }
                }

            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the parents associated to the individuals: {}", e.getMessage(), e);
            }

        }

        if (!sampleVersions.isEmpty()) {
            // Obtain all those samples

            List<Long> uidList = new ArrayList<>(sampleVersions.size());
            List<Integer> versionList = new ArrayList<>(sampleVersions.size());
            sampleVersions.forEach(s -> {
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "__");
                uidList.add(Long.valueOf(split[0]));
                versionList.add(Integer.valueOf(split[1]));
            });

            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.UID.key(), uidList)
                    .append(SampleDBAdaptor.QueryParams.VERSION.key(), versionList);
            List<Document> sampleList;
            try {
                if (user != null) {
                    query.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                    sampleList = sampleDBAdaptor.nativeGet(studyUid, query, sampleQueryOptions, user).getResults();
                } else {
                    sampleList = sampleDBAdaptor.nativeGet(query, sampleQueryOptions).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the samples associated to the individuals: {}", e.getMessage(), e);
                return;
            }

            // Map each sample uid - version to the sample entry
            Map<String, Document> sampleMap = new HashMap<>(sampleList.size());
            sampleList.forEach(sample ->
                    sampleMap.put(String.valueOf(sample.get(IndividualDBAdaptor.QueryParams.UID.key())) + "__"
                            + String.valueOf(sample.get(IndividualDBAdaptor.QueryParams.VERSION.key())), sample)
            );

            // Add the samples obtained to the corresponding individuals
            individualListBuffer.forEach(individual -> {
                List<Document> tmpSampleList = new ArrayList<>();
                List<Document> samples = (List<Document>) individual.get(IndividualMongoDBAdaptor.QueryParams.SAMPLES.key());

                samples.forEach(s -> {
                        String uid = String.valueOf(s.get(IndividualDBAdaptor.QueryParams.UID.key()));
                        String version = String.valueOf(s.get(IndividualDBAdaptor.QueryParams.VERSION.key()));
                        String key = uid + "__" + version;

                        // If the samples has been returned... (it might have not been fetched due to permissions issues)
                        if (sampleMap.containsKey(key)) {
                            tmpSampleList.add(sampleMap.get(key));
                        }
                });

                individual.put(IndividualMongoDBAdaptor.QueryParams.SAMPLES.key(), tmpSampleList);
            });
        }
    }

    private void addParentToMap(Map<Long, List<Document>> individualMap, Document parent) {
        if (parent != null && parent.size() > 0) {
            Long uid = parent.getLong("uid");
            if (uid != null && uid > 0) {
                if (!individualMap.containsKey(uid)) {
                    individualMap.put(uid, new ArrayList<>());
                }
                individualMap.get(uid).add(parent);
            }
        }
    }

    private QueryOptions createSampleQueryOptions() {
        QueryOptions queryOptions = new QueryOptions(NATIVE_QUERY, true);

        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            List<String> includeList = new ArrayList<>();
            for (String include : currentIncludeList) {
                if (include.startsWith(IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".")) {
                    includeList.add(include.replace(IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".", ""));
                }
            }
            if (!includeList.isEmpty()) {
                // If we only have include uid or version, there is no need for an additional query so we will set current options to
                // native query
                boolean includeAdditionalFields = includeList.stream().anyMatch(
                        field -> !field.equals(SampleDBAdaptor.QueryParams.VERSION.key())
                                && !field.equals(SampleDBAdaptor.QueryParams.UID.key())
                );
                if (includeAdditionalFields) {
                    includeList.add(SampleDBAdaptor.QueryParams.VERSION.key());
                    includeList.add(SampleDBAdaptor.QueryParams.UID.key());
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
                if (exclude.startsWith(IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".")) {
                    String replace = exclude.replace(IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".", "");
                    if (!IndividualDBAdaptor.QueryParams.VERSION.key().equals(replace)
                            && !IndividualDBAdaptor.QueryParams.UID.key().equals(replace)) {
                        excludeList.add(replace);
                    }
                }
            }
            if (!excludeList.isEmpty()) {
                queryOptions.put(QueryOptions.EXCLUDE, excludeList);
            }
        }
        if (options.containsKey(Constants.FLATTENED_ANNOTATIONS)) {
            queryOptions.put(Constants.FLATTENED_ANNOTATIONS, options.getBoolean(Constants.FLATTENED_ANNOTATIONS));
        }

        return queryOptions;
    }

}
