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

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.AnnotationSetDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotationConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.summaries.FeatureCount;
import org.opencb.opencga.core.models.summaries.VariableSummary;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.addCompQueryFilter;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.getOperator;
import static org.opencb.opencga.catalog.managers.AnnotationSetManager.ANNOTATIONS;
import static org.opencb.opencga.catalog.managers.AnnotationSetManager.ANNOTATION_SETS;

/**
 * Created by pfurio on 07/07/16.
 */
public abstract class AnnotationMongoDBAdaptor<T> extends MongoDBAdaptor implements AnnotationSetDBAdaptor<T> {

    private final AnnotationConverter annotationConverter;

    AnnotationMongoDBAdaptor(Logger logger) {
        super(logger);

        this.annotationConverter = new AnnotationConverter();
    }

    protected abstract MongoDBCollection getCollection();

    public enum AnnotationSetParams implements QueryParam {
        INTERNAL_ANNOTATION_SETS("customInternalAnnotationSets", TEXT_ARRAY, ""),
        ANNOTATION_SETS("customAnnotationSets", TEXT_ARRAY, ""),
        PRIVATE_VARIABLE_SET_MAP("_vsMap", TEXT_ARRAY, ""),
        PRIVATE_INTERNAL_VARIABLE_SET_MAP("_ivsMap", TEXT_ARRAY, ""),

        // The variables stored as will appear inside the array
        ID("id", TEXT, ""),
        VALUE("value", TEXT, ""),
        VARIABLE_SET_ID("vs", DOUBLE, ""),
        ANNOTATION_SET_NAME("as", TEXT, ""),
        ARRAY_LEVEL("_al", INTEGER_ARRAY, ""),
        COUNT_ELEMENTS("_c", INTEGER_ARRAY, ""),

        // The variables with the full path
        ANNOTATION_SETS_ID(ANNOTATION_SETS.key() + "." + ID.key(), TEXT, ""),
        ANNOTATION_SETS_VALUE(ANNOTATION_SETS.key() + "." + VALUE.key(), TEXT, ""),
        ANNOTATION_SETS_VARIABLE_SET_ID(ANNOTATION_SETS.key() + "." + VARIABLE_SET_ID.key(), DOUBLE, ""),
        ANNOTATION_SETS_ANNOTATION_SET_NAME(ANNOTATION_SETS.key() + "." + ANNOTATION_SET_NAME.key(), TEXT, ""),
        ANNOTATION_SETS_ARRAY_LEVEL(ANNOTATION_SETS.key() + "." + ARRAY_LEVEL.key(), INTEGER_ARRAY, ""),
        ANNOTATION_SETS_COUNT_ELEMENTS(ANNOTATION_SETS.key() + "." + COUNT_ELEMENTS.key(), INTEGER_ARRAY, "");

        private static Map<String, AnnotationSetParams> map;
        static {
            map = new LinkedMap();
            for (AnnotationSetParams params : AnnotationSetParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        AnnotationSetParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, AnnotationSetParams> getMap() {
            return map;
        }

        public static AnnotationSetParams getParam(String key) {
            return map.get(key);
        }
    }

    public void createAnnotationSetForMigration(Object id, VariableSet variableSet, AnnotationSet annotationSet)
            throws CatalogDBException {
        // Check if there already exists an annotation set with the same name
        DataResult<Long> count = getCollection().count(
                new Document()
                        .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSet.getId())
                        .append("_id", id));
        if (count.getNumMatches() > 0) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getId());
        }

        if (variableSet.isUnique()) {
            // Check if the variableset has been already annotated with a different annotation set
            count = getCollection().count(
                    new Document()
                            .append(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), annotationSet.getVariableSetId())
                            .append("_id", id));
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Repeated annotation for a unique VariableSet");
            }
        }

        List<Document> documentList = annotationConverter.annotationToDB(variableSet, annotationSet.getId(),
                annotationSet.getAnnotations());

        // Insert the annotation set in the database
        Bson query = Filters.and(
                Filters.eq("_id", id),
                Filters.eq(AnnotationSetParams.ANNOTATION_SET_NAME.key(), new Document("$ne", annotationSet.getId()))
        );
        Bson update = new Document()
                .append("$addToSet", new Document(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$each", documentList)))
                .append("$set", new Document(AnnotationSetParams.PRIVATE_VARIABLE_SET_MAP.key() + "." + variableSet.getUid(),
                        variableSet.getId()));
        DataResult result = getCollection().update(query, update, null);

        if (result.getNumUpdated() != 1) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getId());
        }
    }

    /**
     * Remove all possible include/exclude annotation options from the query options to do the query properly.
     *
     * @param queryOptions QueryOptions object right before the mongo query is performed.
     * @return A final QueryOptions object excluding the annotation projections as these should only be considered by the converters
     * afterwards.
     */
    public QueryOptions removeAnnotationProjectionOptions(QueryOptions queryOptions) {
        if (queryOptions == null) {
            return new QueryOptions();
        }

        QueryOptions options = new QueryOptions(queryOptions);

        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> projectionList = options.getAsStringList(QueryOptions.EXCLUDE, ",");
            List<String> finalProjectionList = new ArrayList<>();

            for (String projection : projectionList) {
                if (ANNOTATION_SETS.equals(projection)) {
                    finalProjectionList.add(AnnotationSetParams.ANNOTATION_SETS.key());
                    finalProjectionList.add(AnnotationSetParams.INTERNAL_ANNOTATION_SETS.key());
                } else if (!projection.startsWith(Constants.ANNOTATION + ".")
                        && !projection.startsWith(Constants.ANNOTATION_SET_NAME + ".")
                        && !projection.startsWith(Constants.VARIABLE_SET + ".")) {
                    finalProjectionList.add(projection);
                }
            }

            if (finalProjectionList.isEmpty()) {
                options.remove(QueryOptions.EXCLUDE);
            } else {
                options.put(QueryOptions.EXCLUDE, finalProjectionList);
            }
        }

        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> projectionList = options.getAsStringList(QueryOptions.INCLUDE, ",");
            List<String> finalProjectionList = new ArrayList<>();

            boolean includeAnnotation = false;
            for (String projection : projectionList) {
                if (!projection.startsWith(Constants.ANNOTATION + ".") && !projection.startsWith(Constants.ANNOTATION_SET_NAME + ".")
                        && !projection.startsWith(Constants.VARIABLE_SET + ".")) {
                    if (ANNOTATION_SETS.equals(projection)) {
                        includeAnnotation = true;
                    } else {
                        finalProjectionList.add(projection);
                    }
                } else {
                    includeAnnotation = true;
                }
            }

            if (includeAnnotation) {
                // We need to specify we need to include the annotation sets in order to filter them properly afterwards with the converters
                finalProjectionList.add(AnnotationSetParams.ANNOTATION_SETS.key());
                finalProjectionList.add(AnnotationSetParams.INTERNAL_ANNOTATION_SETS.key());
                finalProjectionList.add(AnnotationSetParams.PRIVATE_VARIABLE_SET_MAP.key());
                finalProjectionList.add(AnnotationSetParams.PRIVATE_INTERNAL_VARIABLE_SET_MAP.key());
            }

            if (finalProjectionList.isEmpty()) {
                options.remove(QueryOptions.INCLUDE);
            } else {
                options.put(QueryOptions.INCLUDE, finalProjectionList);
            }
        }

        return options;
    }

    /**
     * Checks if the query contains any annotation query.
     *
     * @param query Query object.
     * @return whether query contains an annotation query or not.
     */
    public boolean containsAnnotationQuery(Query query) {
        if (query == null || query.isEmpty()) {
            return false;
        }
        return query.containsKey(Constants.ANNOTATION);
    }

    OpenCGAResult<? extends Annotable> updateAnnotationSets(ClientSession clientSession, long entryId, ObjectMap parameters,
                                                         List<VariableSet> variableSetList, QueryOptions options, boolean isVersioned)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
        long startTime = startQuery();

        if (actionMap.containsKey(ANNOTATION_SETS)) {
            List<AnnotationSet> annotationSetList = (List<AnnotationSet>) parameters.get(ANNOTATION_SETS);

            ParamUtils.BasicUpdateAction action = ParamUtils.BasicUpdateAction.from(actionMap, ANNOTATION_SETS,
                    ParamUtils.BasicUpdateAction.ADD);

            if (annotationSetList == null) {
                return OpenCGAResult.empty();
            }

            // Create or remove a new annotation set
            if (action == ParamUtils.BasicUpdateAction.ADD || action == ParamUtils.BasicUpdateAction.SET) {
                // 1. Check the annotation set ids are not in use
                validateNewAnnotations(clientSession, entryId, annotationSetList, variableSetList, isVersioned);

                // 2. Obtain the list of documents that need to be inserted
                List<Document> annotationDocumentList = getNewAnnotationList(annotationSetList, variableSetList);
                List<Document> internalAnnotationDocumentList = getNewAnnotationList(annotationSetList, variableSetList, true);

                if (action == ParamUtils.BasicUpdateAction.SET) {
                    if (CollectionUtils.isEmpty(internalAnnotationDocumentList)) {
                        // 2.1 Remove all user existing annotations
                        removeAllAnnotationSets(clientSession, entryId, isVersioned);
                    } else {
                        // 2.1 Remove all internal existing annotations
                        removeAllAnnotationSets(clientSession, entryId, isVersioned, true);
                    }
                }

                if (CollectionUtils.isEmpty(internalAnnotationDocumentList)) {
                    // 3. Insert the list of documents
                    addNewAnnotations(clientSession, entryId, annotationDocumentList, isVersioned);

                    // 4. Set variable set map uid - id
                    addPrivateVariableMap(clientSession, entryId, getPrivateVariableMapToSet(annotationSetList, variableSetList),
                            isVersioned);
                } else {
                    // 3. Insert the list of documents
                    addNewAnnotations(clientSession, entryId, internalAnnotationDocumentList, isVersioned, true);

                    // 4. Set variable set map uid - id
                    addPrivateVariableMap(clientSession, entryId, getPrivateVariableMapToSet(annotationSetList, variableSetList),
                            isVersioned, true);
                }

            } else if (action == ParamUtils.BasicUpdateAction.REMOVE) {
                // Action = REMOVE

                // 0. Obtain the annotationSet to be removed to know the variableSet being annotated
                OpenCGAResult<Document> queryResult = nativeGet(new Query(PRIVATE_UID, entryId), new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(ANNOTATION_SETS)));

                if (queryResult.getNumResults() != 1) {
                    throw new CatalogDBException("Unexpected error. Could not obtain the entry information. The annotationSet could "
                            + "not be removed.");
                }

                Map<String, VariableSet> variableSetMap = new HashMap<>();
                for (VariableSet variableSet : variableSetList) {
                    variableSetMap.put(variableSet.getId(), variableSet);
                }

                List<Document> annotationList = (List<Document>) queryResult.first().get(AnnotationSetParams.ANNOTATION_SETS.key());
                List<Document> internalAnnotationList = (List<Document>) queryResult.first()
                        .get(AnnotationSetParams.INTERNAL_ANNOTATION_SETS.key());
                Map<String, String> annotationSetIdVariableSetUidMap = new HashMap<>();
                // This variable will contain a map of variable set ids pointing to all the annotationSet ids using the variable set
                Map<String, Set<String>> variableSetAnnotationsets = new HashMap<>();
                Set<String> existingAnnotationSets = new HashSet<>();
                for (Document document : annotationList) {
                    String variableSetId = String.valueOf(document.getLong(AnnotationSetParams.VARIABLE_SET_ID.key()));
                    String annSetId = document.getString(AnnotationSetParams.ANNOTATION_SET_NAME.key());

                    annotationSetIdVariableSetUidMap.put(annSetId, variableSetId);

                    if (!variableSetAnnotationsets.containsKey(variableSetId)) {
                        variableSetAnnotationsets.put(variableSetId, new HashSet<>());
                    }
                    variableSetAnnotationsets.get(variableSetId).add(annSetId);
                    existingAnnotationSets.add(annSetId);
                }
                if (internalAnnotationList != null) {
                    for (Document document : internalAnnotationList) {
                        String variableSetId = String.valueOf(document.getLong(AnnotationSetParams.VARIABLE_SET_ID.key()));
                        String annSetId = document.getString(AnnotationSetParams.ANNOTATION_SET_NAME.key());

                        annotationSetIdVariableSetUidMap.put(annSetId, variableSetId);

                        if (!variableSetAnnotationsets.containsKey(variableSetId)) {
                            variableSetAnnotationsets.put(variableSetId, new HashSet<>());
                        }
                        variableSetAnnotationsets.get(variableSetId).add(annSetId);
                        existingAnnotationSets.add(annSetId);
                    }
                }

                for (AnnotationSet annotationSet : annotationSetList) {
                    if (StringUtils.isNotEmpty(annotationSet.getId())) {
                        if (!existingAnnotationSets.contains(annotationSet.getId())) {
                            throw new CatalogDBException("Could not delete: AnnotationSet " + annotationSet.getId() + " not found");
                        }

                        // 1. Remove annotationSet
                        removeAnnotationSetByAnnotationSetId(clientSession, entryId, annotationSet.getId(), isVersioned);

                        String variableSetId = annotationSetIdVariableSetUidMap.get(annotationSet.getId());
                        // Remove the annotation set from the variableSetAnnotationsets
                        variableSetAnnotationsets.get(variableSetId).remove(annotationSet.getId());

                        // Only if the variableSet is not being annotated by any annotationSet, we can remove the private variableSetMap
                        if (variableSetAnnotationsets.get(variableSetId).isEmpty()) {
                            // 2. Unset variable set map uid - id
                            Map<String, String> variableSetMapToRemove = new HashMap<>();
                            variableSetMapToRemove.put(variableSetId, null);
                            removePrivateVariableMap(clientSession, entryId, variableSetMapToRemove, isVersioned);
                        }
                    } else if (StringUtils.isNotEmpty(annotationSet.getVariableSetId())) {
                        VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());
                        if (variableSet == null) {
                            throw new CatalogDBException("Could not delete AnnotationSet. VariableSet '" + annotationSet.getVariableSetId()
                                    + "' not found");
                        }

                        if (variableSetAnnotationsets.containsKey(String.valueOf(variableSet.getUid()))) {
                            // Only perform the update if variable set is actually present

                            // Private VariableSet to remove
                            Map<String, String> variableSetMapToRemove = new HashMap<>();
                            variableSetMapToRemove.put(String.valueOf(variableSet.getUid()), null);

                            if (!variableSet.isInternal()) {
                                // Remove all annotationSets
                                removeAnnotationSetByVariableSetId(clientSession, entryId, variableSet.getUid(), isVersioned);
                                removePrivateVariableMap(clientSession, entryId, variableSetMapToRemove, isVersioned);
                            } else {
                                // Remove all annotationSets
                                removeAnnotationSetByVariableSetId(clientSession, entryId, variableSet.getUid(), isVersioned, true);
                                removePrivateVariableMap(clientSession, entryId, variableSetMapToRemove, isVersioned, true);
                            }
                        }
                    } else {
                        throw new CatalogDBException("Could not delete AnnotationSet. AnnotationSet 'id' or 'variableSetId' not defined.");
                    }
                }

            }
        } else if (actionMap.containsKey(ANNOTATIONS)) {
            // Update annotation
            AnnotationSet annotationSet = ((List<AnnotationSet>) parameters.get(ANNOTATION_SETS)).get(0);

            // 1. Get list of annotations to be inserted
            List<Document> annotationDocumentList = getNewAnnotationList(Collections.singletonList(annotationSet), variableSetList);

            // 2. Remove all the existing annotations of the annotation set
            removeAnnotationSetByAnnotationSetId(clientSession, entryId, annotationSet.getId(), isVersioned);

            // 3. Add new list of annotations
            addNewAnnotations(clientSession, entryId, annotationDocumentList, isVersioned);
        }

        return endWrite(startTime, 1, 1, new ArrayList<>());
    }

    private void removePrivateVariableMap(ClientSession clientSession, long entryId, Map<String, String> privateVariableMapToSet,
                                          boolean isVersioned) throws CatalogDBException {
        removePrivateVariableMap(clientSession, entryId, privateVariableMapToSet, isVersioned, false);
    }

    private void removePrivateVariableMap(ClientSession clientSession, long entryId, Map<String, String> privateVariableMapToSet,
                                          boolean isVersioned, boolean isInternal) throws CatalogDBException {
        Document queryDocument = new Document(PRIVATE_UID, entryId);
        if (isVersioned) {
            queryDocument.append(LAST_OF_VERSION, true);
        }

        for (Map.Entry<String, String> entry : privateVariableMapToSet.entrySet()) {
            // We only want to remove the private variable map if it is not currently in use by any annotation set
            queryDocument.append(AnnotationSetParams.VARIABLE_SET_ID.key(), new Document("$ne", Long.parseLong(entry.getKey())));

            Bson unset;
            if (!isInternal) {
                unset = Updates.unset(AnnotationSetParams.PRIVATE_VARIABLE_SET_MAP.key() + "." + entry.getKey());
            } else {
                unset = Updates.unset(AnnotationSetParams.PRIVATE_INTERNAL_VARIABLE_SET_MAP.key() + "." + entry.getKey());
            }

            DataResult result = getCollection().update(clientSession, queryDocument, unset, new QueryOptions());
            if (result.getNumUpdated() < 1 && result.getNumMatches() == 1) {
                throw new CatalogDBException("Could not remove private map information");
            }
        }
    }

    private void addPrivateVariableMap(ClientSession clientSession, long entryId, Map<String, String> variableMap, boolean isVersioned)
            throws CatalogDBException {
        addPrivateVariableMap(clientSession, entryId, variableMap, isVersioned, false);
    }

    private void addPrivateVariableMap(ClientSession clientSession, long entryId, Map<String, String> variableMap, boolean isVersioned,
                                       boolean isInternal) throws CatalogDBException {
        Document queryDocument = new Document(PRIVATE_UID, entryId);
        if (isVersioned) {
            queryDocument.append(LAST_OF_VERSION, true);
        }

        String key = isInternal ? AnnotationSetParams.PRIVATE_INTERNAL_VARIABLE_SET_MAP.key()
                : AnnotationSetParams.PRIVATE_VARIABLE_SET_MAP.key();
        List<Bson> setMap = new ArrayList<>(variableMap.size());
        for (Map.Entry<String, String> entry : variableMap.entrySet()) {
            setMap.add(Updates.set(key + "." + entry.getKey(), entry.getValue()));
        }

        DataResult result = getCollection().update(clientSession, queryDocument, Updates.combine(setMap), new QueryOptions("multi", true));
        if (result.getNumUpdated() < 1 && result.getNumMatches() == 0) {
            throw new CatalogDBException("Could not add new private map information");
        }
    }

    private Map<String, String> getPrivateVariableMapToSet(List<AnnotationSet> annotationSetList, List<VariableSet> variableSetList) {
        Map<String, VariableSet> variableSetMap = new HashMap<>();
        for (VariableSet variableSet : variableSetList) {
            variableSetMap.put(variableSet.getId(), variableSet);
        }

        Map<String, String> privateVariableMap = new HashMap<>();
        for (AnnotationSet annotationSet : annotationSetList) {
            VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());
            privateVariableMap.put(String.valueOf(variableSet.getUid()), variableSet.getId());
        }

        return privateVariableMap;
    }

    private void removeAllAnnotationSets(ClientSession clientSession, long entryId, boolean isVersioned) throws CatalogDBException {
        removeAllAnnotationSets(clientSession, entryId, isVersioned, false);
    }

    private void removeAllAnnotationSets(ClientSession clientSession, long entryId, boolean isVersioned, boolean internal)
            throws CatalogDBException {
        Document queryDocument = new Document(PRIVATE_UID, entryId);
        if (isVersioned) {
            queryDocument.append(LAST_OF_VERSION, true);
        }

        // We empty the annotation sets list and the private map
        Bson bsonUpdate;
        if (!internal) {
            bsonUpdate = Updates.combine(
                    Updates.set(AnnotationSetParams.ANNOTATION_SETS.key(), Collections.emptyList()),
                    Updates.set(AnnotationSetParams.PRIVATE_VARIABLE_SET_MAP.key(), Collections.emptyMap())
            );
        } else {
            bsonUpdate = Updates.combine(
                    Updates.set(AnnotationSetParams.INTERNAL_ANNOTATION_SETS.key(), Collections.emptyList()),
                    Updates.set(AnnotationSetParams.PRIVATE_INTERNAL_VARIABLE_SET_MAP.key(), Collections.emptyMap())
            );
        }

        DataResult result = getCollection().update(clientSession, queryDocument, bsonUpdate, new QueryOptions());
        if (result.getNumUpdated() < 1 && result.getNumMatches() == 0) {
            throw new CatalogDBException("Could not remove all annotationSets");
        }
    }

    private void addNewAnnotations(ClientSession clientSession, long entryId, List<Document> annotationDocumentList, boolean isVersioned)
            throws CatalogDBException {
        addNewAnnotations(clientSession, entryId, annotationDocumentList, isVersioned, false);
    }

    private void addNewAnnotations(ClientSession clientSession, long entryId, List<Document> annotationDocumentList, boolean isVersioned,
                                   boolean isInternal) throws CatalogDBException {
        Document queryDocument = new Document(PRIVATE_UID, entryId);
        if (isVersioned) {
            queryDocument.append(LAST_OF_VERSION, true);
        }

        String key = isInternal ? AnnotationSetParams.INTERNAL_ANNOTATION_SETS.key() : AnnotationSetParams.ANNOTATION_SETS.key();
        Bson push = Updates.addEachToSet(key, annotationDocumentList);

        DataResult result = getCollection().update(clientSession, queryDocument, push, new QueryOptions("multi", true));
        if (result.getNumUpdated() < 1) {
            throw new CatalogDBException("Could not add new annotations");
        }
    }

    private void removeAnnotationSetByAnnotationSetId(ClientSession clientSession, long entryId, String annotationSetId,
                                                      boolean isVersioned) throws CatalogDBException {
        Document queryDocument = new Document(PRIVATE_UID, entryId);
        if (isVersioned) {
            queryDocument.append(LAST_OF_VERSION, true);
        }

        Bson pull = Updates.pull(AnnotationSetParams.ANNOTATION_SETS.key(),
                new Document(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSetId));

        DataResult result = getCollection().update(clientSession, queryDocument, pull, new QueryOptions("multi", true));
        if (result.getNumUpdated() < 1) {
            throw new CatalogDBException("Could not delete the annotation set");
        }
    }

    private void removeAnnotationSetByVariableSetId(ClientSession clientSession, long entryId, long variableSetUid, boolean isVersioned)
            throws CatalogDBException {
        removeAnnotationSetByVariableSetId(clientSession, entryId, variableSetUid, isVersioned, false);
    }

    private void removeAnnotationSetByVariableSetId(ClientSession clientSession, long entryId, long variableSetUid, boolean isVersioned,
                                                    boolean isInternal) throws CatalogDBException {
        Document queryDocument = new Document(PRIVATE_UID, entryId);
        if (isVersioned) {
            queryDocument.append(LAST_OF_VERSION, true);
        }

        Bson pull;
        if (!isInternal) {
            pull = Updates.pull(AnnotationSetParams.ANNOTATION_SETS.key(),
                    new Document(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetUid));
        } else {
            pull = Updates.pull(AnnotationSetParams.INTERNAL_ANNOTATION_SETS.key(),
                    new Document(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetUid));
        }

        DataResult result = getCollection().update(clientSession, queryDocument, pull, new QueryOptions("multi", true));
        if (result.getNumMatches() > 0 && result.getNumUpdated() < 1) {
            throw new CatalogDBException("Could not delete the annotation set");
        }
    }

    private void validateNewAnnotations(ClientSession clientSession, long entryId, List<AnnotationSet> annotationSetList,
                                        List<VariableSet> variableSetList, boolean isVersioned) throws CatalogDBException {
        // 1. Check for duplicates in the list of annotation sets
        Set<String> annotationSetIds = new HashSet<>();
        for (AnnotationSet annotationSet : annotationSetList) {
            if (annotationSetIds.contains(annotationSet.getId())) {
                throw new CatalogDBException("Found more than one annotationSet with same id " + annotationSet.getId());
            }
            annotationSetIds.add(annotationSet.getId());
        }

        Map<String, VariableSet> variableSetMap = new HashMap<>();
        for (VariableSet variableSet : variableSetList) {
            variableSetMap.put(variableSet.getId(), variableSet);
        }

        for (AnnotationSet annotationSet : annotationSetList) {
            // Check if there already exists an annotation set with the same id
            Document query = new Document()
                    .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSet.getId())
                    .append(PRIVATE_UID, entryId);
            if (isVersioned) {
                query.put(LAST_OF_VERSION, true);
            }

            DataResult<Long> count = getCollection().count(clientSession, query);
            if (count.getNumMatches() > 0) {
                throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
            }

            VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());
            if (variableSet.isUnique()) {
                query = new Document()
                        .append(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), annotationSet.getVariableSetId())
                        .append(PRIVATE_UID, entryId);
                if (isVersioned) {
                    query.put(LAST_OF_VERSION, true);
                }

                // Check if the variableSet has been already annotated with a different annotation set
                count = getCollection().count(clientSession, query);
                if (count.getNumMatches() > 0) {
                    throw new CatalogDBException("Repeated annotation for a unique VariableSet");
                }
            }
        }

    }

    private List<Document> getNewAnnotationList(List<AnnotationSet> annotationSetList, List<VariableSet> variableSetList) {
        return getNewAnnotationList(annotationSetList, variableSetList, false);
    }

    private List<Document> getNewAnnotationList(List<AnnotationSet> annotationSetList, List<VariableSet> variableSetList,
                                                boolean internal) {
        List<Document> annotationList = new ArrayList<>();

        Map<String, VariableSet> variableSetMap = new HashMap<>();
        for (VariableSet variableSet : variableSetList) {
            variableSetMap.put(variableSet.getId(), variableSet);
        }

        // Convert the annotations to the list of documents
        for (AnnotationSet annotationSet : annotationSetList) {
            VariableSet variableSet = variableSetMap.get(annotationSet.getVariableSetId());
            if (internal == variableSet.isInternal()) {
                annotationList.addAll(annotationConverter.annotationToDB(variableSet, annotationSet));
            }
        }

        return annotationList;
    }

    public OpenCGAResult addVariableToAnnotations(long variableSetId, Variable variable) throws CatalogDBException {
        long startTime = startQuery();

        // We generate the generic document that should be inserted
        Set<Variable> setOfVariables = new HashSet<>();
        setOfVariables.add(variable);

        VariableSet variableSet = new VariableSet()
                .setUid(variableSetId)
                .setVariables(setOfVariables);

        // This can actually be a list of more than 1 element if the new variable added is an object. In such a case, we could have
        // something like a: {b1: xxx, b2: yyy}; and we would have returned two elements. One with a.b1 and another one with a.b2
        // We pass null as the annotation set name because we will set it later, once we know the different annotation set names it can
        // take
        List<Document> documentList = annotationConverter.annotationToDB(variableSet, null, Collections.emptyMap());

        // Obtain the annotation names of the annotations that are using the variableSet variableSetId
        List<Bson> aggregation = new ArrayList<>(4);
        aggregation.add(Aggregates.match(Filters.eq(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), variableSetId)));
        aggregation.add(Aggregates.unwind("$" + AnnotationSetParams.ANNOTATION_SETS.key()));
        aggregation.add(Aggregates.project(Projections.include(
                AnnotationSetParams.ANNOTATION_SETS_ANNOTATION_SET_NAME.key(), AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key()
        )));
        aggregation.add(Aggregates.match(Filters.eq(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), variableSetId)));
        // We group by the annotation set name to get all the different ids
        aggregation.add(new Document("$group", new Document("_id", "$" + AnnotationSetParams.ANNOTATION_SETS_ANNOTATION_SET_NAME.key())));

        DataResult<Document> aggregationResult = getCollection().aggregate(aggregation, null);

        // Store the different annotation names in the set
        Set<String> annotationNames = new HashSet<>(aggregationResult.getNumResults());
        for (Document document : aggregationResult.getResults()) {
            annotationNames.add(document.getString("_id"));
        }

        // Construct the query dynamically for each different annotation set and make the update
        long matchCount = 0;
        long modifiedCount = 0;
        Bson bsonQuery;
        for (String annotationId : annotationNames) {
            bsonQuery = Filters.elemMatch(AnnotationSetParams.ANNOTATION_SETS.key(), Filters.and(
                    Filters.eq(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId),
                    Filters.eq(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationId)
            ));

            // Add the annotation set key-value to the documents that will be pushed
            for (Document document : documentList) {
                document.put(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationId);
            }

            // Prepare the update event
            Bson update = new Document("$addToSet", new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
                    new Document("$each", documentList)));

            DataResult result = getCollection().update(bsonQuery, update, new QueryOptions(MongoDBCollection.MULTI, true));
            modifiedCount += result.getNumUpdated();
            matchCount += result.getNumMatches();
        }

        return endWrite(startTime, matchCount, modifiedCount, new ArrayList<>());
    }

    /**
     * Remove the whole annotation matching the variable to be removed. If the variable is a complex object, it will remove all the
     * entries the object might have.
     * Example: "personal: { name, telephone, address ...}"
     *    - If fieldId is personal, it will remove all the existing entries for personal.name, personal.telephone, personal.address, etc.
     *    - If fieldId is personal.name, it will only remove the existing entries for personal.name
     *
     * @param variableSetId Variable set id.
     * @param fieldId Field id corresponds with the variable name whose annotations have to be removed.
     * @return A OpenCGAResult object.
     * @throws CatalogDBException if there is any unexpected error.
     */
    public OpenCGAResult removeAnnotationField(long variableSetId, String fieldId) throws CatalogDBException {
        long startTime = startQuery();
//        List<Document> aggregateResult = getAnnotationDocuments(variableSetId, fieldId);

        Document pull = new Document("$pull",
                new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
                        new Document()
                                .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId)
                                .append(AnnotationSetParams.ID.key(), Pattern.compile("^" + fieldId))
                ));

        Document query = new Document(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$elemMatch",
                new Document()
                        .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId)
                        .append(AnnotationSetParams.ID.key(), Pattern.compile("^" + fieldId))));

        DataResult result = getCollection().update(query, pull, new QueryOptions("multi", true));
        if (result.getNumUpdated() == 0 && result.getNumMatches() > 0) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "}: An unexpected error happened when extracting the "
                    + "annotations for the variable " + fieldId + ". Please, report this error to the OpenCGA developers.");
        }

        return new OpenCGAResult(result);
    }

    public OpenCGAResult<VariableSummary> getAnnotationSummary(long studyId, long variableSetId) throws CatalogDBException {
        long startTime = startQuery();

        List<Bson> aggregation = new ArrayList<>(6);
        aggregation.add(new Document("$match", new Document(PRIVATE_STUDY_UID, studyId)));
        aggregation.add(new Document("$project", new Document(AnnotationSetParams.ANNOTATION_SETS.key(), 1)));
        aggregation.add(new Document("$unwind", "$" + AnnotationSetParams.ANNOTATION_SETS.key()));
//        aggregation.add(new Document("$unwind", "$" + AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS.key()));

        aggregation.add(new Document("$match",
                new Document(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), variableSetId))
        );
        aggregation.add(new Document("$group",
                        new Document("_id", new Document()
                                .append("name", "$" + AnnotationSetParams.ANNOTATION_SETS_ID.key())
                                .append("value", "$" + AnnotationSetParams.ANNOTATION_SETS_VALUE.key()))
                                .append("count", new Document("$sum", 1))
                )
        );
        aggregation.add(new Document("$sort", new Document("_id.name", -1).append("count", -1)));

        List<Document> result = getCollection().aggregate(aggregation, new QueryOptions()).getResults();

        List<VariableSummary> variableSummaryList = new ArrayList<>();

        List<FeatureCount> featureCountList = new ArrayList<>();
        VariableSummary v = new VariableSummary();

        for (Document document : result) {
            Document id = (Document) document.get("_id");
            String name = id.getString("name");
            Object value = id.get("value");
            int count = document.getInteger("count");

            if (!name.equals(v.getName())) {
                featureCountList = new ArrayList<>();
                v = new VariableSummary(name, featureCountList);
                variableSummaryList.add(v);
            }

            featureCountList.add(new FeatureCount(value, count));
        }

        return endQuery(startTime, variableSummaryList);
    }

    public Document createAnnotationQuery(String annotations, ObjectMap variableTypeMap) throws CatalogDBException {
        Document document = new Document();

        if (StringUtils.isNotEmpty(annotations)) {
            // Annotation Filter
            final String sepAnd = ";";
            String[] annotationArray = StringUtils.split(annotations, sepAnd);

            List<Document> documentList = new ArrayList<>();
            for (String annotation : annotationArray) {
                Matcher matcher = AnnotationUtils.ANNOTATION_PATTERN.matcher(annotation);

                if (matcher.find()) {
                    // Split the annotation by key - value
                    String annotationSet = matcher.group(1);
                    String variableSet = matcher.group(2);
                    String key = matcher.group(3);
                    String valueString = matcher.group(4);

                    if (annotation.startsWith(Constants.ANNOTATION_SET_NAME)) {
                        String operator = getOperator(valueString);
                        String value = valueString.replace(operator, "");
                        switch (operator) {
                            case "=":
                            case "==":
                                // Return as long as that annotationSetName is present
                                documentList.add(new Document(AnnotationSetParams.ANNOTATION_SETS_ANNOTATION_SET_NAME.key(), value));
                                break;
                            case "===":
                                // Return if there is only that annotationSetName
                                documentList.add(new Document("$and", Arrays.asList(
                                        new Document(AnnotationSetParams.ANNOTATION_SETS_ANNOTATION_SET_NAME.key(), value),
                                        new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
                                                new Document("$not", new Document("$elemMatch",
                                                        new Document(AnnotationSetParams.ANNOTATION_SET_NAME.key(),
                                                                new Document("$ne", value)))))
                                )));
                                break;
                            case "!":
                            case "!=":
                                // If there is another annotationSet with a name different than the value, it will match
                                documentList.add(new Document("$or", Arrays.asList(
                                        new Document(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$size", 0)),
                                        new Document(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$elemMatch",
                                                new Document(AnnotationSetParams.ANNOTATION_SET_NAME.key(), new Document("$ne", value)))
                                        ))));
                                break;
                            case "!==":
                                // Even if there is another annotationSet with a name different than the value, it will NOT match
                                documentList.add(new Document(AnnotationSetParams.ANNOTATION_SETS_ANNOTATION_SET_NAME.key(),
                                        new Document("$ne", value)));
                                break;
                            default:
                                throw new CatalogDBException("Internal error. Operator of " + annotation + " not understood. Accepted"
                                        + " operators are =, ==, ===, !, !=, !==");
                        }
                    } else if (annotation.startsWith(Constants.VARIABLE_SET)) {
                        String operator = getOperator(valueString);
                        long value = Long.parseLong(valueString.replace(operator, ""));
                        switch (operator) {
                            case "=":
                            case "==":
                                // Return as long as that variable set is annotated
                                documentList.add(new Document(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), value));
                                break;
                            case "===":
                                // Only return if there is only that variable set annotated
                                documentList.add(new Document("$and", Arrays.asList(
                                        new Document(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), value),
                                        new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
                                                new Document("$not", new Document("$elemMatch",
                                                        new Document(AnnotationSetParams.VARIABLE_SET_ID.key(),
                                                                new Document("$ne", value)))))
                                )));
                                break;
                            case "!":
                            case "!=":
                                // If there is another variableSet different than the value, it will match
                                documentList.add(new Document("$or", Arrays.asList(
                                        new Document(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$size", 0)),
                                        new Document(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$elemMatch",
                                                new Document(AnnotationSetParams.VARIABLE_SET_ID.key(), new Document("$ne", value)))
                                ))));
                                break;
                            case "!==":
                                // Even if there is another variableSet different than the value, it will NOT match
                                documentList.add(new Document(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(),
                                        new Document("$ne", value)));
                                break;
                            default:
                                throw new CatalogDBException("Internal error. Operator of " + annotation + " not understood. Accepted"
                                        + " operators are =, ==, ===, !, !=, !==");
                        }
                    } else {
                        // Annotation...

                        if (StringUtils.isNotEmpty(annotationSet)) {
                            annotationSet = annotationSet.replace("@", "");
                        }

                        // Remove the : at the end of the variableSet
                        variableSet = variableSet.replace(":", "");

                        if (variableTypeMap == null || variableTypeMap.isEmpty()) {
                            logger.error("Internal error: The variableTypeMap is null or empty {}", variableTypeMap);
                            throw new CatalogDBException("Internal error. Could not build the annotation query");
                        }
                        QueryParam.Type type = variableTypeMap.get(variableSet + ":" + key, QueryParam.Type.class);
                        if (type == null) {
                            logger.error("Internal error: Could not find the type of the variable {}:{}", variableSet, key);
                            throw new CatalogDBException("Internal error. Could not find the type of the variable " + variableSet + ":"
                                    + key);
                        }
                        if (!variableTypeMap.containsKey(variableSet + "__isInternal")) {
                            logger.error("Internal error: Could not find if the VariableSet '{}' is internal", variableSet);
                            throw new CatalogDBException("Internal error. Could not find if the VariableSet '" + variableSet
                                    + "' is internal");
                        }
                        boolean isInternal = variableTypeMap.getBoolean(variableSet + "__isInternal");

                        List<Document> valueList;
                        try {
                             valueList = addCompQueryFilter(type, AnnotationSetParams.VALUE.key(), Arrays.asList(valueString.split(",")),
                                     new ArrayList<>());
                        } catch (CatalogDBException e) {
                            throw new CatalogDBException("Variable " + key + ": " + e.getMessage(), e);
                        }

                        Document queryDocument = new Document()
                                .append(AnnotationSetParams.ID.key(), key)
                                .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableTypeMap.getLong(variableSet));
                        if (StringUtils.isNotEmpty(annotationSet)) {
                            queryDocument.append(AnnotationSetParams.ANNOTATION_SET_NAME.key, annotationSet);
                        }
                        queryDocument.putAll(valueList.get(0));

                        if (!isInternal) {
                            // Add the query to the document query list
                            documentList.add(new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
                                    new Document("$elemMatch", queryDocument)));
                        } else {
                            // Add the query to the document query list
                            documentList.add(new Document(AnnotationSetParams.INTERNAL_ANNOTATION_SETS.key(),
                                    new Document("$elemMatch", queryDocument)));
                        }
                    }
                } else {
                    throw new CatalogDBException("Annotation " + annotation + " could not be parsed to a query.");
                }
            }

            if (documentList.size() > 1) {
                document = new Document("$and", documentList);
            } else {
                document = documentList.get(0);
            }
        }

        return document;
    }

}
