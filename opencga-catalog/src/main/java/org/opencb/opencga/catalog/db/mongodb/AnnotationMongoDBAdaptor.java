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

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.AnnotationSetDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotationConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.Annotable;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Variable;
import org.opencb.opencga.core.models.VariableSet;
import org.opencb.opencga.core.models.summaries.FeatureCount;
import org.opencb.opencga.core.models.summaries.VariableSummary;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.addCompQueryFilter;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.getOperator;
import static org.opencb.opencga.catalog.managers.AnnotationSetManager.ANNOTATION_SETS;
import static org.opencb.opencga.catalog.managers.AnnotationSetManager.ANNOTATION_SET_ACTION;

/**
 * Created by pfurio on 07/07/16.
 */
public abstract class AnnotationMongoDBAdaptor<T> extends MongoDBAdaptor implements AnnotationSetDBAdaptor<T> {

    private final AnnotationConverter annotationConverter;

    AnnotationMongoDBAdaptor(Logger logger) {
        super(logger);

        this.annotationConverter = new AnnotationConverter();
    }

    protected abstract AnnotableConverter<? extends Annotable> getConverter();

    protected abstract MongoDBCollection getCollection();

    public enum AnnotationSetParams implements QueryParam {
        ANNOTATION_SETS("customAnnotationSets", TEXT_ARRAY, ""),

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

    public QueryResult<AnnotationSet> createAnnotationSet(long id, VariableSet variableSet, AnnotationSet annotationSet)
            throws CatalogDBException {

        long startTime = startQuery();
        // Check if there already exists an annotation set with the same name
        QueryResult<Long> count = getCollection().count(
                new Document()
                        .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSet.getName())
                        .append(PRIVATE_ID, id));
        if (count.first() > 0) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
        }

        if (variableSet.isUnique()) {
            // Check if the variableset has been already annotated with a different annotation set
            count = getCollection().count(
                    new Document()
                            .append(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), annotationSet.getVariableSetId())
                            .append(PRIVATE_ID, id));
            if (count.first() > 0) {
                throw new CatalogDBException("Repeated annotation for a unique VariableSet");
            }
        }

        List<Document> documentList = annotationConverter.annotationToDB(variableSet, annotationSet.getName(),
                annotationSet.getAnnotations());

        // Insert the annotation set in the database
        Bson query = Filters.and(
                Filters.eq(PRIVATE_ID, id),
                Filters.eq(AnnotationSetParams.ANNOTATION_SET_NAME.key(), new Document("$ne", annotationSet.getName()))
        );
        Bson update = new Document("$addToSet", new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
                new Document("$each", documentList)));
        QueryResult<UpdateResult> queryResult = getCollection().update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
        }
        return endQuery("Create annotation set", startTime, getAnnotationSet(id, annotationSet.getName()));
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

    public ObjectMap prepareAnnotationUpdate(long entryId, ObjectMap parameters, List<VariableSet> variableSetList)
            throws CatalogDBException {
        ObjectMap retMap = new ObjectMap()
                .append(AnnotationSetManager.Action.DELETE_ANNOTATION.name(), parameters.getString(Constants.DELETE_ANNOTATION))
                .append(AnnotationSetManager.Action.DELETE_ANNOTATION_SET.name(), parameters.getString(Constants.DELETE_ANNOTATION_SET));

        if (!parameters.containsKey(ANNOTATION_SETS) || parameters.get(ANNOTATION_SETS) == null) {
            return retMap;
        }

        List<AnnotationSet> annotationSetList = (List<AnnotationSet>) parameters.get(ANNOTATION_SETS);
        if (annotationSetList == null || annotationSetList.isEmpty() || variableSetList.isEmpty()) {
            return retMap;
        }

        Map<String, AnnotationSetManager.Action> annotationSetAction =
                (Map<String, AnnotationSetManager.Action>) parameters.get(ANNOTATION_SET_ACTION);

        Map<Long, VariableSet> variableSetMap = new HashMap<>();
        for (VariableSet variableSet : variableSetList) {
            variableSetMap.put(variableSet.getId(), variableSet);
        }

        List<Document> createAnnotations = new ArrayList<>();
        List<Document> updateAnnotations = new ArrayList<>();

        for (AnnotationSet annotationSet : annotationSetList) {
            List<Document> documentList = annotationConverter.annotationToDB(variableSetMap.get(annotationSet.getVariableSetId()),
                    annotationSet);

            switch (annotationSetAction.get(annotationSet.getName())) {
                case CREATE:
                    // Check if there already exists an annotation set with the same name
                    QueryResult<Long> count = getCollection().count(
                            new Document()
                                    .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSet.getName())
                                    .append(PRIVATE_ID, entryId));
                    if (count.first() > 0) {
                        throw CatalogDBException.alreadyExists("AnnotationSet", "name", annotationSet.getName());
                    }

                    if (variableSetMap.get(annotationSet.getVariableSetId()).isUnique()) {
                        // Check if the variableset has been already annotated with a different annotation set
                        count = getCollection().count(
                                new Document()
                                        .append(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), annotationSet.getVariableSetId())
                                        .append(PRIVATE_ID, entryId));
                        if (count.first() > 0) {
                            throw new CatalogDBException("Repeated annotation for a unique VariableSet");
                        }
                    }

                    createAnnotations.addAll(documentList);
                    break;
                case UPDATE:
                    updateAnnotations.addAll(documentList);
                    break;
                default:
                    throw new CatalogDBException("Unknown AnnotationSet action found: " + annotationSetAction.get(annotationSet.getName()));
            }
        }

        return retMap
                .append(AnnotationSetManager.Action.CREATE.name(), createAnnotations)
                .append(AnnotationSetManager.Action.UPDATE.name(), updateAnnotations);
    }

    private QueryResult<? extends Annotable> convertToDataModelQueryResult(QueryResult<Document> documentQueryResult,
                                                                           @Nullable String annotationSetName, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }

        List<Annotable> annotableList = new ArrayList<>(documentQueryResult.getNumResults());
        for (Document document : documentQueryResult.getResult()) {
            if (StringUtils.isNotEmpty(annotationSetName)) {
                List<Map<String, Object>> annotationSets =
                        (List<Map<String, Object>>) document.get(AnnotationSetParams.ANNOTATION_SETS.key());
                if (annotationSets != null && !annotationSets.isEmpty()) {
                    // We remove all the annotations from a different annotation set name
                    annotationSets.removeIf(annotation ->
                            !annotationSetName.equals(annotation.get(AnnotationSetParams.ANNOTATION_SET_NAME.key())));
                }
            }
            // We convert the results to the data models
            annotableList.add(getConverter().convertToDataModelType(document, options));
        }
        return new QueryResult<>(documentQueryResult.getId(), documentQueryResult.getDbTime(),
                documentQueryResult.getNumResults(), documentQueryResult.getNumTotalResults(), documentQueryResult.getWarningMsg(),
                documentQueryResult.getErrorMsg(),
                annotableList);
    }

    public void applyAnnotationUpdates(Long entryId, ObjectMap annotationUpdateMap, boolean isVersioned) throws CatalogDBException {
        if (entryId <= 0 || annotationUpdateMap.isEmpty()) {
            return;
        }

        List<Document> documentList = (List<Document>) annotationUpdateMap.get(AnnotationSetManager.Action.CREATE.name());
        if (documentList != null && !documentList.isEmpty()) {
            // Insert the annotation set in the database
            Document query = new Document(PRIVATE_ID, entryId);
            if (isVersioned) {
                query.append(LAST_OF_VERSION, true);
            }
            Bson update = new Document("$addToSet", new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
                    new Document("$each", documentList)));
            getCollection().update(query, update, null);
        }

        documentList = (List<Document>) annotationUpdateMap.get(AnnotationSetManager.Action.UPDATE.name());
        if (documentList != null && !documentList.isEmpty()) {
            // We go annotation per annotation
            for (Document document : documentList) {
                String annotationId = document.getString(AnnotationSetParams.ID.key());
                Object annotationValue = document.get(AnnotationSetParams.VALUE.key());
                String annotationName = document.getString(AnnotationSetParams.ANNOTATION_SET_NAME.key());
                long variableSetId = document.getLong(AnnotationSetParams.VARIABLE_SET_ID.key());

                Document queryDocument = new Document()
                        .append(PRIVATE_ID, entryId)
                        .append(AnnotationSetParams.ANNOTATION_SETS.key(),
                                new Document("$elemMatch", new Document()
                                        .append(AnnotationSetParams.ID.key(), annotationId)
                                        .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId)
                                        .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationName)));
                if (isVersioned) {
                    queryDocument.append(LAST_OF_VERSION, true);
                }

                String annotationPrivateId = annotationConverter.getAnnotationPrivateId(String.valueOf(variableSetId), annotationName,
                        annotationId);

                // We make a first attempt trying only to update the value of the annotation
                Document updateDocument = new Document("$set", new Document()
                        .append(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + AnnotationSetParams.VALUE.key(), annotationValue)
                        .append(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + annotationPrivateId, annotationValue)
                );

                QueryResult<UpdateResult> update = getCollection().update(queryDocument, updateDocument, null);

                if (update.first().getMatchedCount() == 0) {
                    // That annotation did not exist already, so we need to add that new annotation to the array
                    queryDocument = new Document()
                            .append(PRIVATE_ID, entryId)
                            .append(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$not",
                                    new Document("$elemMatch", new Document()
                                            .append(AnnotationSetParams.ID.key(), annotationId)
                                            .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId)
                                            .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationName))));
                    if (isVersioned) {
                        queryDocument.append(LAST_OF_VERSION, true);
                    }

                    updateDocument = new Document("$push", new Document(AnnotationSetParams.ANNOTATION_SETS.key(), document));

                    update = getCollection().update(queryDocument, updateDocument, null);

                    if (update.first().getMatchedCount() == 0) {
                        throw new CatalogDBException("Internal error. No matching entry to update annotations");
                    }
                    if (update.first().getModifiedCount() == 0) {
                        throw new CatalogDBException("Internal error. Could not add new annotation to annotationSet");
                    }
                }
            }
        }

        String annotationSetToRemove = (String) annotationUpdateMap.get(AnnotationSetManager.Action.DELETE_ANNOTATION_SET.name());
        if (StringUtils.isNotEmpty(annotationSetToRemove)) {
            Document queryDocument = new Document(PRIVATE_ID, entryId);
            if (isVersioned) {
                queryDocument.append(LAST_OF_VERSION, true);
            }

            Bson pull = Updates.pull(AnnotationSetParams.ANNOTATION_SETS.key(),
                    new Document(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSetToRemove));

            QueryResult<UpdateResult> update = getCollection().update(queryDocument, pull, new QueryOptions("multi", true));
            if (update.first().getModifiedCount() < 1) {
                throw new CatalogDBException("Could not delete the annotation set");
            }
        }

        String annotationToRemoveList = (String) annotationUpdateMap.get(AnnotationSetManager.Action.DELETE_ANNOTATION.name());
        if (StringUtils.isNotEmpty(annotationToRemoveList)) {
            for (String annotationToRemove : StringUtils.split(annotationToRemoveList, ",")) {
                String[] split = StringUtils.split(annotationToRemove, ":");
                if (split.length != 2) {
                    throw new CatalogDBException("Cannot delete annotation " + annotationToRemove
                            + ". The format is annotationSetName:variable.");
                }
                String annotationSetName = split[0];
                String variable = split[1];

                Document queryDocument = new Document()
                        .append(PRIVATE_ID, entryId)
                        .append(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$elemMatch",
                                new Document()
                                        .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSetName)
                                        .append(AnnotationSetParams.ID.key(), Pattern.compile("^" + variable))));
                if (isVersioned) {
                    queryDocument.append(LAST_OF_VERSION, true);
                }

                Bson pull = Updates.pull(AnnotationSetParams.ANNOTATION_SETS.key(), new Document()
                        .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSetName)
                        .append(AnnotationSetParams.ID.key(), Pattern.compile("^" + variable)));

                QueryResult<UpdateResult> update = getCollection().update(queryDocument, pull, new QueryOptions("multi", true));
                if (update.first().getMatchedCount() > 0 && update.first().getModifiedCount() < 1) {
                    throw new CatalogDBException("Could not delete the annotation " + annotationToRemove);
                }
            }
        }
    }
//
//    public QueryResult<AnnotationSet> updateAnnotationSet(long id, VariableSet variableSet, AnnotationSet annotationSet)
//            throws CatalogDBException {
//        long startTime = startQuery();
//
//        Map<String, Object> annotations = annotationSet.getAnnotations();
//        List<Document> documentList = annotationConverter.annotationToDB(variableSet, annotationSet.getName(), annotations);
//
//        // We go annotation per annotation
//        for (Document document : documentList) {
//            String annotationId = document.getString(AnnotationSetParams.ID.key());
//            Object annotationValue = document.get(AnnotationSetParams.VALUE.key());
//            String annotationName = document.getString(AnnotationSetParams.ANNOTATION_SET_NAME.key());
//            long variableSetId = document.getLong(AnnotationSetParams.VARIABLE_SET_ID.key());
//
//            Document queryDocument = new Document()
//                    .append(PRIVATE_ID, id)
//                    .append(AnnotationSetParams.ANNOTATION_SETS.key(),
//                            new Document("$elemMatch", new Document()
//                                    .append(AnnotationSetParams.ID.key(), annotationId)
//                                    .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId)
//                                    .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationName)));
//
//            // We make a first attempt trying only to update the value of the annotation
//            Document updateDocument = new Document("$set",
//                    new Document(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + AnnotationSetParams.VALUE.key(), annotationValue));
//
//            QueryResult<UpdateResult> update = getCollection().update(queryDocument, updateDocument, null);
//
//            if (update.first().getMatchedCount() == 0) {
//                // That annotation did not exist already, so we need to add that new annotation to the array
//                queryDocument = new Document()
//                        .append(PRIVATE_ID, id)
//                        .append(AnnotationSetParams.ANNOTATION_SETS.key(), new Document("$not",
//                                new Document("$elemMatch", new Document()
//                                        .append(AnnotationSetParams.ID.key(), annotationId)
//                                        .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId)
//                                        .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationName))));
//
//                updateDocument = new Document("$push", new Document(AnnotationSetParams.ANNOTATION_SETS.key(), document));
//
//                update = getCollection().update(queryDocument, updateDocument, null);
//
//                if (update.first().getMatchedCount() == 0) {
//                    throw new CatalogDBException("Internal error. No matching entry to update annotations");
//                }
//                if (update.first().getModifiedCount() == 0) {
//                    throw new CatalogDBException("Internal error. Could not add new annotation to annotationSet");
//                }
//            }
//
//        }
//
//        // If documentList size is lower than the size of annotations, that must be because there are some annotations that are
//        // intended to be removed
//        if (documentList.size() < annotations.size()) {
//
//            Set<String> annotationKeysSet = documentList.stream()
//                    .map(x -> x.getString(AnnotationSetParams.ID.key()))
//                    .collect(Collectors.toSet());
//
//            for (Map.Entry<String, Object> annotationEntry : annotations.entrySet()) {
//                String annotationKey = annotationEntry.getKey();
//
//                if (!annotationKeysSet.contains(annotationKey)) {
//                    // Remove this annotation from mongo
//                    Document queryDocument = new Document()
//                            .append(PRIVATE_ID, id)
//                            .append(AnnotationSetParams.ANNOTATION_SETS.key(),
//                                    new Document("$elemMatch", new Document()
//                                            .append(AnnotationSetParams.ID.key(), annotationKey)
//                                            .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSet.getId())
//                                            .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSet.getName())));
//
//                    Document pull = new Document("$pull",
//                            new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
//                                    new Document()
//                                            .append(AnnotationSetParams.ID.key(), annotationKey)
//                                            .append(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSet.getId())
//                                            .append(AnnotationSetParams.ANNOTATION_SET_NAME.key(), annotationSet.getName())
//                            ));
//
//                    getCollection().update(queryDocument, pull, null);
//
//                }
//            }
//        }
//
//        return endQuery("Update annotation set", startTime, getAnnotationSet(id, annotationSet.getName(), null));
//    }

    public QueryResult<Long> addVariableToAnnotations(long variableSetId, Variable variable) throws CatalogDBException {
        long startTime = startQuery();

        // We generate the generic document that should be inserted
        Set<Variable> setOfVariables = new HashSet<>();
        setOfVariables.add(variable);

        VariableSet variableSet = new VariableSet()
                .setId(variableSetId)
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

        QueryResult<Document> aggregationResult = getCollection().aggregate(aggregation, null);

        // Store the different annotation names in the set
        Set<String> annotationNames = new HashSet<>(aggregationResult.getNumResults());
        for (Document document : aggregationResult.getResult()) {
            annotationNames.add(document.getString("_id"));
        }

        // Construct the query dynamically for each different annotation set and make the update
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

            modifiedCount += getCollection().update(bsonQuery, update, new QueryOptions(MongoDBCollection.MULTI, true)).first()
                    .getModifiedCount();
        }

        return endQuery("Add annotation", startTime, Collections.singletonList(modifiedCount));
    }
//
//    // TODO
//    public QueryResult<Long> renameAnnotationField(long variableSetId, String oldName, String newName) throws CatalogDBException {
//        long startTime = startQuery();
//        long renamedAnnotations = 0;
////        List<Document> aggregateResult = getAnnotationDocuments(variableSetId, oldName);
////
////        if (aggregateResult.size() > 0) {
////            // Each document will be a cohort, sample, individual or family
////            for (Document entity : aggregateResult) {
////                Object entityId = entity.get(AnnotationSetParams.ID.key());
////                Document annotationSet = ((Document) entity.get(AnnotationSetParams.ANNOTATION_SETS.key()));
////
////                String annotationSetName = annotationSet.getString(AnnotationSetParams.NAME.key());
////
////                // Build a query to look for the particular annotations
////                Bson bsonQuery = Filters.and(
////                        Filters.eq(PRIVATE_ID, entityId),
////                        Filters.eq(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSetName),
////                        Filters.eq(AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS_NAME.key(), oldName)
////                );
////
////                // And extract those annotations from the annotation set
////                Bson update = Updates.pull(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + AnnotationSetParams.ANNOTATIONS.key(),
////                        Filters.eq(AnnotationSetParams.NAME.key(), oldName));
////
////                QueryResult<UpdateResult> queryResult = getCollection().update(bsonQuery, update, null);
////
////                if (queryResult.first().getModifiedCount() != 1) {
////                    throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - AnnotationSet {name: "
////                            + annotationSet.getString(AnnotationSetParams.NAME.key()) + "} - An unexpected error happened when "
////                            + "extracting the annotation " + oldName + ". Please, report this error to the OpenCGA developers.");
////                }
////
////                // Obtain the value of the annotation
////                Object value = ((Document) annotationSet.get(AnnotationSetParams.ANNOTATIONS.key()))
// .get(AnnotationSetParams.VALUE.key());
////
////                // Create a new annotation with the new id and the former value
////                Document annotation = new Document(newName, value);
////
////                bsonQuery = Filters.and(
////                        Filters.eq(PRIVATE_ID, entityId),
////                        Filters.eq(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSetName)
////                );
////
////                // Push the again the annotation with the new name
////                update = Updates.push(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + AnnotationSetParams.ANNOTATIONS.key(),
////                        MongoDBUtils.getMongoDBDocument(annotation, "Annotation"));
////                queryResult = getCollection().update(bsonQuery, update, null);
////
////                if (queryResult.first().getModifiedCount() != 1) {
////                    throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - AnnotationSet {name: "
////                            + annotationSetName + "} - A critical error happened when trying to rename the annotation " + oldName
////                            + ". Please, report this error to the OpenCGA developers.");
////                }
////                renamedAnnotations += 1;
////
////            }
////        }
//        return endQuery("Rename annotation name", startTime, Collections.singletonList(renamedAnnotations));
//    }

    /**
     * Remove the whole annotation matching the variable to be removed. If the variable is a complex object, it will remove all the
     * entries the object might have.
     * Example: "personal: { name, telephone, address ...}"
     *    - If fieldId is personal, it will remove all the existing entries for personal.name, personal.telephone, personal.address, etc.
     *    - If fieldId is personal.name, it will only remove the existing entries for personal.name
     *
     * @param variableSetId Variable set id.
     * @param fieldId Field id corresponds with the variable name whose annotations have to be removed.
     * @return A query result containing the number of annotations that were removed.
     * @throws CatalogDBException if there is any unexpected error.
     */
    public QueryResult<Long> removeAnnotationField(long variableSetId, String fieldId) throws CatalogDBException {
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

        QueryResult<UpdateResult> queryResult = getCollection().update(query, pull, new QueryOptions("multi", true));
        if (queryResult.first().getModifiedCount() == 0 && queryResult.first().getMatchedCount() > 0) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "}: An unexpected error happened when extracting the "
                    + "annotations for the variable " + fieldId + ". Please, report this error to the OpenCGA developers.");
        }

        return endQuery("Remove annotation", startTime, Collections.singletonList(queryResult.first().getModifiedCount()));
    }

    public QueryResult<VariableSummary> getAnnotationSummary(long studyId, long variableSetId) throws CatalogDBException {
        long startTime = startQuery();

        List<Bson> aggregation = new ArrayList<>(6);
        aggregation.add(new Document("$match", new Document(PRIVATE_STUDY_ID, studyId)));
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

        List<Document> result = getCollection().aggregate(aggregation, new QueryOptions()).getResult();

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

        return endQuery("Get Annotation summary", startTime, variableSummaryList);
    }

    public Document createAnnotationQuery(String annotations, ObjectMap variableTypeMap) throws CatalogDBException {
        Document document = new Document();

        if (StringUtils.isNotEmpty(annotations)) {
            // Annotation Filter
            final String sepAnd = ";";
            String[] annotationArray = StringUtils.split(annotations, sepAnd);

            List<Document> documentList = new ArrayList<>();
            for (String annotation : annotationArray) {
                Matcher matcher = AnnotationSetManager.ANNOTATION_PATTERN.matcher(annotation);

                if (matcher.find()) {
                    // Split the annotation by key - value
                    String variableSet = matcher.group(1);
                    String key = matcher.group(2);
                    String valueString = matcher.group(3);

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

                        List<Document> valueList;
                        try {
                             valueList = addCompQueryFilter(type, AnnotationSetParams.VALUE.key(), Arrays.asList(valueString.split(",")),
                                     new ArrayList<>());
                        } catch (CatalogDBException e) {
                            throw new CatalogDBException("Variable " + key + ": " + e.getMessage(), e);
                        }

                        Document queryDocument = new Document()
                                .append(AnnotationSetParams.ID.key(), key)
                                .append(AnnotationSetParams.VARIABLE_SET_ID.key(), Long.parseLong(variableSet));
                        queryDocument.putAll(valueList.get(0));

                        // Add the query to the document query list
                        documentList.add(new Document(AnnotationSetParams.ANNOTATION_SETS.key(),
                                new Document("$elemMatch", queryDocument)));
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
