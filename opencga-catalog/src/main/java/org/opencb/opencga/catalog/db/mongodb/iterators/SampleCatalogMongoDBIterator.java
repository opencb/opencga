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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.IndividualMongoDBAdaptor;
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

public class SampleCatalogMongoDBIterator<E> extends AnnotableCatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private IndividualMongoDBAdaptor individualDBAdaptor;
    private QueryOptions individualQueryOptions;

    private Queue<Document> sampleListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;

    public SampleCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                        AnnotableConverter<? extends Annotable> converter, Function<Document, Document> filter,
                                        IndividualMongoDBAdaptor individualDBAdaptor, QueryOptions options) {
        this(mongoCursor, clientSession, converter, filter, individualDBAdaptor, 0, null, options);
    }

    public SampleCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                        AnnotableConverter<? extends Annotable> converter, Function<Document, Document> filter,
                                        IndividualMongoDBAdaptor individualDBAdaptor, long studyUid, String user, QueryOptions options) {
        super(mongoCursor, clientSession, converter, filter, options);

        this.user = user;
        this.studyUid = studyUid;

        this.individualDBAdaptor = individualDBAdaptor;
        this.individualQueryOptions = createIndividualQueryOptions();

        this.sampleListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(SampleCatalogMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = sampleListBuffer.remove();

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
        if (sampleListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !sampleListBuffer.isEmpty();
    }

    private void fetchNextBatch() {
        Map<Long, Document> sampleUidMap = new HashMap<>(BUFFER_SIZE);

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document sampleDocument = (Document) mongoCursor.next();

            if (user != null && studyUid <= 0) {
                studyUid = sampleDocument.getLong(PRIVATE_STUDY_UID);
            }

            sampleListBuffer.add(sampleDocument);
            counter++;

            // Extract the sample uids
            if (!options.getBoolean(NATIVE_QUERY) && !options.getBoolean("lazy")) {
                // Extract the sample uid
                sampleUidMap.put(sampleDocument.getLong(SampleDBAdaptor.QueryParams.UID.key()), sampleDocument);
            }
        }

        if (!sampleUidMap.isEmpty()) {
            // Obtain all the related individuals
            Query query = new Query(IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleUidMap.keySet());
            List<Document> individualList;
            try {
                if (user != null) {
                    individualList = individualDBAdaptor.nativeGet(clientSession, studyUid, query, individualQueryOptions, user)
                            .getResults();
                } else {
                    individualList = individualDBAdaptor.nativeGet(clientSession, query, individualQueryOptions).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the individuals containing the samples: {}", e.getMessage(), e);
                return;
            }

            // Add the individuals to the sample attributes
            individualList.forEach(individual -> {
                List<Document> samples = (List<Document>) individual.get(IndividualMongoDBAdaptor.QueryParams.SAMPLES.key());

                if (samples != null) {
                    individual.remove(IndividualMongoDBAdaptor.QueryParams.SAMPLES.key());
                    samples.forEach(s -> {
                        long uid = s.getLong(SampleDBAdaptor.QueryParams.UID.key());

                        Document sample = sampleUidMap.get(uid);
                        if (sample != null) { // If the sample exists
                            Document attributes = (Document) sample.get(SampleDBAdaptor.QueryParams.ATTRIBUTES.key());
                            if (attributes == null) {
                                attributes = new Document();
                                sample.put(SampleDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
                            }
                            // We add the individual to the attributes field
                            attributes.put("OPENCGA_INDIVIDUAL", individual);
                        }
                    });
                }
            });
        }
    }

    private QueryOptions createIndividualQueryOptions() {
        QueryOptions queryOptions = new QueryOptions(NATIVE_QUERY, true);

        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            List<String> includeList = new ArrayList<>();
            for (String include : currentIncludeList) {
                if (include.startsWith(SampleDBAdaptor.QueryParams.INDIVIDUAL.key() + ".")) {
                    includeList.add(include.replace(SampleDBAdaptor.QueryParams.INDIVIDUAL.key() + ".", ""));
                }
            }
            if (!includeList.isEmpty()) {
                // We need to include also the sample uids to retrieve the data
                includeList.add(IndividualDBAdaptor.QueryParams.SAMPLES.key());

                queryOptions.put(QueryOptions.INCLUDE, includeList);
            }
        }
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> currentExcludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            List<String> excludeList = new ArrayList<>();
            for (String exclude : currentExcludeList) {
                if (exclude.startsWith(SampleDBAdaptor.QueryParams.INDIVIDUAL.key() + ".")) {
                    String replace = exclude.replace(SampleDBAdaptor.QueryParams.INDIVIDUAL.key() + ".", "");
                    excludeList.add(replace);
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
