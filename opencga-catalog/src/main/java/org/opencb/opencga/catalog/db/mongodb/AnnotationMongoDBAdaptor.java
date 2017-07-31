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
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.models.summaries.FeatureCount;
import org.opencb.opencga.catalog.models.summaries.VariableSummary;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.checkStudyPermission;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.addCompQueryFilter;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.fixAnnotationQuery;

/**
 * Created by pfurio on 07/07/16.
 */
abstract class AnnotationMongoDBAdaptor extends MongoDBAdaptor {

    private static final String VARIABLE_SETS = "variableSets";

    AnnotationMongoDBAdaptor(Logger logger) {
        super(logger);
    }

    protected abstract GenericDocumentComplexConverter<? extends Annotable> getConverter();

    protected abstract MongoDBCollection getCollection();

    enum AnnotationSetParams implements QueryParam {
        ID("id", TEXT, ""),
        VARIABLE_SET_ID("variableSetId", DOUBLE, ""),

        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        ANNOTATION_SETS_NAME("annotationSets.name", TEXT, ""),
        ANNOTATION_SETS_VARIABLE_SET_ID("annotationSets.variableSetId", DECIMAL, ""),
        ANNOTATION_SETS_ANNOTATIONS("annotationSets.annotations", TEXT_ARRAY, ""),
        ANNOTATION_SETS_ANNOTATIONS_NAME("annotationSets.annotations.name", TEXT, ""),
        ANNOTATION_SETS_ANNOTATIONS_VALUE("annotationSets.annotations.value", TEXT, ""),

        ANNOTATIONS("annotations", TEXT_ARRAY, ""),
        NAME("name", TEXT, ""),
        VALUE("value", TEXT, "");

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

    public QueryResult<AnnotationSet> createAnnotationSet(long id, AnnotationSet annotationSet) throws CatalogDBException {
        long startTime = startQuery();
        // Check if there already exists an annotation set with the same name
        QueryResult<Long> count = getCollection().count(
                new Document()
                        .append(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSet.getName())
                        .append(PRIVATE_ID, id));

        if (count.first() > 0) {
            throw CatalogDBException.alreadyExists("AnnotationSet", AnnotationSetParams.NAME.key(), annotationSet.getName());
        }

        Document document = MongoDBUtils.getMongoDBDocument(annotationSet, "AnnotationSet");
        document.put("variableSetId", annotationSet.getVariableSetId());

        // Insert the annotation set in the database
        Bson query = Filters.and(
                Filters.eq(PRIVATE_ID, id),
                Filters.eq(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), new Document("$ne", annotationSet.getName()))
        );
        Bson update = new Document("$push", new Document(AnnotationSetParams.ANNOTATION_SETS.key(), document));
        QueryResult<UpdateResult> queryResult = getCollection().update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw CatalogDBException.alreadyExists("AnnotationSet", AnnotationSetParams.NAME.key(), annotationSet.getName());
        }
        return endQuery("Create annotation set", startTime, getAnnotationSet(id, annotationSet.getName()));
    }

    public QueryResult<AnnotationSet> searchAnnotationSet(AbstractManager.MyResourceId resource, long variableSetId,
                                                          @Nullable String annotation, String studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        QueryResult<? extends Annotable> aggregate = baseSearchAnnotationSet(resource, variableSetId, annotation, studyPermission);
        List<AnnotationSet> annotationSets = new ArrayList<>(aggregate.getNumResults());
        for (Annotable annotable : aggregate.getResult()) {
            annotationSets.add((AnnotationSet) annotable.getAnnotationSets().get(0));
        }

        return endQuery("Search annotation set", startTime, annotationSets);
    }

    public QueryResult<ObjectMap> searchAnnotationSetAsMap(AbstractManager.MyResourceId resource, long variableSetId,
                                                           @Nullable String annotation, String studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        QueryResult<? extends Annotable> aggregate = baseSearchAnnotationSet(resource, variableSetId, annotation, studyPermission);
        List<ObjectMap> annotationSets = new ArrayList<>(aggregate.getNumResults());
        for (Annotable annotable : aggregate.getResult()) {
            annotationSets.add((ObjectMap) annotable.getAnnotationSetAsMap().get(0));
        }

        return endQuery("Search annotation set", startTime, annotationSets);
    }

    private QueryResult<? extends Annotable> baseSearchAnnotationSet(AbstractManager.MyResourceId resource, long variableSetId,
                                                                     @Nullable String annotation, String studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        Map<String, Variable> variableMap = null;
        Document filter = new Document();

        if (variableSetId > 0) {
            filter.append(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), variableSetId);
            if (StringUtils.isNotEmpty(annotation)) {
                variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                        .getVariables().stream().collect(Collectors.toMap(Variable::getName, Function.identity()));
            }
        }
        if (StringUtils.isNotEmpty(annotation)) {
            Query query = new Query("annotation", annotation);
            fixAnnotationQuery(query);
            Document annotQuery = createAnnotationQueryFilter(query, variableMap);
            if (annotQuery != null) {
                filter.putAll(annotQuery);
            }
        }

        return commonGetAnnotationSet(resource, filter, null, studyPermission);
    }

    private Document createAnnotationQueryFilter(Query query, Map<String, Variable> variableMap) throws CatalogDBException {
        // Annotation Filter
        final String sepOr = ",";

        List<Document> annotationFilterList = new ArrayList<>(query.size());
        for (Map.Entry<String, Object> objectEntry : query.entrySet()) {
            String annotationKey;
            if (objectEntry.getKey().startsWith("annotation.")) {
                annotationKey = objectEntry.getKey().substring("annotation.".length());
            } else {
                throw new CatalogDBException("Wrong annotation query. Expects: {\"annotation.<variable>\" , <operator><value> } ");
            }
            String annotationValue = query.getString(objectEntry.getKey());

            final String variableId;
            final String route;
            if (annotationKey.contains(".")) {
                String[] variableIdRoute = annotationKey.split("\\.", 2);
                variableId = variableIdRoute[0];
                route = "." + variableIdRoute[1];
            } else {
                variableId = annotationKey;
                route = "";
            }
            String[] values = annotationValue.split(sepOr);

            QueryParam.Type type = QueryParam.Type.TEXT;

            if (variableMap != null) {
                Variable variable = variableMap.get(variableId);
                if (variable == null) {
                    throw new CatalogDBException("Variable \"" + variableId + "\" not found in variableSet ");
                }
                Variable.VariableType variableType = variable.getType();
                if (variable.getType() == Variable.VariableType.OBJECT) {
                    String[] routes = route.split("\\.");
                    for (String r : routes) {
                        if (variable.getType() != Variable.VariableType.OBJECT) {
                            throw new CatalogDBException("Unable to query variable " + annotationKey);
                        }
                        if (variable.getVariableSet() != null) {
                            Map<String, Variable> subVariableMap = variable.getVariableSet().stream()
                                    .collect(Collectors.toMap(Variable::getName, Function.<Variable>identity()));
                            if (subVariableMap.containsKey(r)) {
                                variable = subVariableMap.get(r);
                                variableType = variable.getType();
                            }
                        } else {
                            variableType = Variable.VariableType.TEXT;
                            break;
                        }
                    }
                } else if (variableType == Variable.VariableType.BOOLEAN) {
                    type = QueryParam.Type.BOOLEAN;
                } else if (variableType == Variable.VariableType.DOUBLE) {
                    type = QueryParam.Type.DECIMAL;
                } else if (variableType == Variable.VariableType.INTEGER) {
                    type = QueryParam.Type.INTEGER;
                }
            }

            List<Document> valueList = addCompQueryFilter(type, AnnotationSetParams.VALUE.key() + route, Arrays.asList(values),
                    new ArrayList<>());

            annotationFilterList.add(
                    new Document("$elemMatch", valueList.get(0).append(AnnotationSetParams.NAME.key(), variableId))
            );
        }
        if (annotationFilterList.size() > 0) {
            return new Document(AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS.key(),
                    new Document("$all", annotationFilterList));

//                    Filters.all(AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS.key(), annotationFilterList);
        }

        return null;
    }

    public QueryResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<? extends Annotable> aggregate = commonGetAnnotationSet(id, null, annotationSetName);

        List<AnnotationSet> annotationSets = new ArrayList<>(aggregate.getNumResults());
        for (Annotable annotable : aggregate.getResult()) {
            annotationSets.add((AnnotationSet) annotable.getAnnotationSets().get(0));
        }

        return endQuery("Get annotation set", startTime, annotationSets);
    }

    public QueryResult<AnnotationSet> getAnnotationSet(AbstractManager.MyResourceId resource, @Nullable String annotationSetName,
                                                       String studyPermission) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        QueryResult<? extends Annotable> aggregate = commonGetAnnotationSet(resource, null, annotationSetName, studyPermission);

        List<AnnotationSet> annotationSets = new ArrayList<>(aggregate.getNumResults());
        for (Annotable annotable : aggregate.getResult()) {
            annotationSets.add((AnnotationSet) annotable.getAnnotationSets().get(0));
        }

        return endQuery("Get annotation set", startTime, annotationSets);
    }

    public QueryResult<ObjectMap> getAnnotationSetAsMap(AbstractManager.MyResourceId resource, @Nullable String annotationSetName,
                                                        String studyPermission) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();

        QueryResult<? extends Annotable> aggregate = commonGetAnnotationSet(resource, null, annotationSetName, studyPermission);

        List<ObjectMap> annotationSets = new ArrayList<>(aggregate.getNumResults());
        for (Annotable annotable : aggregate.getResult()) {
            annotationSets.add((ObjectMap) annotable.getAnnotationSetAsMap().get(0));
        }

        return endQuery("Get annotation set", startTime, annotationSets);
    }

    private QueryResult<? extends Annotable> commonGetAnnotationSet(long id, Bson queryAnnotation, @Nullable String annotationSetName) {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(new Document(PRIVATE_ID, id)));
//        aggregation.add(Aggregates.project(Projections.include(AnnotationSetParams.ID.key(), AnnotationSetParams.ANNOTATION_SETS.key())));
        aggregation.add(Aggregates.unwind("$" + AnnotationSetParams.ANNOTATION_SETS.key()));
        if (queryAnnotation != null) {
            aggregation.add(Aggregates.match(queryAnnotation));
        }

        if (annotationSetName != null && !annotationSetName.isEmpty()) {
            aggregation.add(Aggregates.match(new Document(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSetName)));
        }

        for (Bson bson : aggregation) {
            try {
                logger.debug("Get annotation: {}", bson.toBsonDocument(Document.class, com.mongodb.MongoClient.getDefaultCodecRegistry()));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        return getCollection().aggregate(aggregation, getConverter(), null);
    }

    private QueryResult<? extends Annotable> commonGetAnnotationSet(AbstractManager.MyResourceId resource, Bson queryAnnotation,
                                                                    @Nullable String annotationSetName, String studyPermission)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(
                new Query(StudyDBAdaptor.QueryParams.ID.key(), resource.getStudyId()), new QueryOptions());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Internal error: Study " + resource.getStudyId() + " not found.");
        }
        Document matchPermissions = getQueryForAuthorisedEntries(queryResult.first(), resource.getUser(), studyPermission,
                "VIEW_ANNOTATIONS");

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(matchPermissions.append(PRIVATE_ID, resource.getResourceId())));
//        aggregation.add(Aggregates.project(Projections.include(AnnotationSetParams.ID.key(), AnnotationSetParams.ANNOTATION_SETS.key())));
        aggregation.add(Aggregates.unwind("$" + AnnotationSetParams.ANNOTATION_SETS.key()));
        if (queryAnnotation != null) {
            aggregation.add(Aggregates.match(queryAnnotation));
        }

        if (annotationSetName != null && !annotationSetName.isEmpty()) {
            aggregation.add(Aggregates.match(new Document(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSetName)));
        }

        for (Bson bson : aggregation) {
            try {
                logger.debug("Get annotation: {}", bson.toBsonDocument(Document.class, com.mongodb.MongoClient.getDefaultCodecRegistry()));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        QueryResult<? extends Annotable> results = getCollection().aggregate(aggregation, getConverter(), null);

        if (results.getNumResults() > 0) {
            // Check if the user has the CONFIDENTIAL PERMISSION
            boolean confidential = checkStudyPermission(queryResult.first(), resource.getUser(),
                            StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS.toString());
            if (!confidential) {
                // If the user does not have the confidential permission, we will have to remove those annotation sets coming from
                // confidential variable sets
                List<Document> variableSets = (List<Document>) queryResult.first().get(VARIABLE_SETS);
                Set<Long> confidentialVariableSets = new HashSet<>();
                for (Document variableSet : variableSets) {
                    if (variableSet.getBoolean("confidential")) {
                        confidentialVariableSets.add(variableSet.getLong("id"));
                    }
                }

                if (confidentialVariableSets.size() > 0) {
                    // The study contains confidential variable sets so we do have to check if any of the annotations come from
                    // confidential variable sets
                    for (Annotable annotable : results.getResult()) {
                        Iterator<AnnotationSet> iterator = annotable.getAnnotationSets().iterator();
                        while (iterator.hasNext()) {
                            AnnotationSet annotationSet = iterator.next();
                            if (confidentialVariableSets.contains(annotationSet.getVariableSetId())) {
                                iterator.remove();
                            }
                        }
                    }
                }
            }
        }

        return results;
    }

    public QueryResult<AnnotationSet> updateAnnotationSet(long id, AnnotationSet annotationSet) throws CatalogDBException {
        long startTime = startQuery();

        // Check if there already exists an annotation set with the same name
        QueryResult<Long> count = getCollection().count(
                new Document()
                        .append(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSet.getName())
                        .append(PRIVATE_ID, id));

        if (count.first() == 0) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationSet.getName());
        }

        Document document = MongoDBUtils.getMongoDBDocument(annotationSet, "AnnotationSet");

        // Insert the annotation set in the database
        Bson query = Filters.and(
                Filters.eq(PRIVATE_ID, id),
                Filters.eq(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSet.getName())
        );
        Bson update = new Document("$set", new Document(AnnotationSetParams.ANNOTATION_SETS.key() + ".$", document));
        QueryResult<UpdateResult> queryResult = getCollection().update(query, update, null);

        if (queryResult.first().getMatchedCount() == 0) {
            throw new CatalogDBException("The annotation set could not be updated. No match found.");
        }

        return endQuery("Update annotation set", startTime, getAnnotationSet(id, annotationSet.getName()));
    }

    public void deleteAnnotationSet(long id, String annotationSetName) throws CatalogDBException {
        QueryResult<AnnotationSet> annotationSet = getAnnotationSet(id, annotationSetName);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw CatalogDBException.idNotFound("Annotation set", annotationSetName);
        }

        Bson eq = Filters.eq(PRIVATE_ID, id);
        Bson pull = Updates.pull(AnnotationSetParams.ANNOTATION_SETS.key(),
                new Document(AnnotationSetParams.NAME.key(), annotationSetName));

        QueryResult<UpdateResult> update = getCollection().update(eq, pull, null);
        if (update.first().getModifiedCount() < 1) {
            throw new CatalogDBException("Could not delete the annotation set");
        }
    }

    public QueryResult<Long> addVariableToAnnotations(long variableSetId, Variable variable) throws CatalogDBException {
        long startTime = startQuery();

        Annotation annotation = new Annotation(variable.getName(), variable.getDefaultValue());

        // Obtain the annotation names of the annotations that are using the variableSet variableSetId
        List<Bson> aggregation = new ArrayList<>(4);
        aggregation.add(Aggregates.match(Filters.eq(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), variableSetId)));
        aggregation.add(Aggregates.unwind("$" + AnnotationSetParams.ANNOTATION_SETS.key()));
        aggregation.add(Aggregates.project(Projections.include(
                AnnotationSetParams.ANNOTATION_SETS_NAME.key(), AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key()
        )));
        aggregation.add(Aggregates.match(Filters.eq(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), variableSetId)));
        QueryResult<Document> aggregationResult = getCollection().aggregate(aggregation, null);

        // Store the different annotation names in the set
        Set<String> annotationNames = new HashSet<>(aggregationResult.getNumResults());
        for (Document document : aggregationResult.getResult()) {
            annotationNames.add((String) ((Document) document.get(AnnotationSetParams.ANNOTATION_SETS.key()))
                    .get(AnnotationSetParams.NAME.key()));
        }

        // Prepare the update event
        Bson update = Updates.push(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + AnnotationSetParams.ANNOTATIONS.key(),
                MongoDBUtils.getMongoDBDocument(annotation, "Annotation"));

        // Construct the query dynamically for each different annotation set and make the update
        long modifiedCount = 0;
        Bson bsonQuery;
        for (String annotationId : annotationNames) {
            bsonQuery = Filters.elemMatch(AnnotationSetParams.ANNOTATION_SETS.key(), Filters.and(
                    Filters.eq(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId),
                    Filters.eq(AnnotationSetParams.NAME.key(), annotationId)
            ));

            modifiedCount += getCollection().update(bsonQuery, update, new QueryOptions(MongoDBCollection.MULTI, true)).first()
                    .getModifiedCount();
        }

        return endQuery("Add annotation", startTime, Collections.singletonList(modifiedCount));
    }

    public QueryResult<Long> renameAnnotationField(long variableSetId, String oldName, String newName) throws CatalogDBException {
        long startTime = startQuery();
        long renamedAnnotations = 0;
        List<Document> aggregateResult = getAnnotationDocuments(variableSetId, oldName);

        if (aggregateResult.size() > 0) {
            // Each document will be a cohort, sample or individual
            for (Document entity : aggregateResult) {
                Object entityId = entity.get(AnnotationSetParams.ID.key());
                Document annotationSet = ((Document) entity.get(AnnotationSetParams.ANNOTATION_SETS.key()));

                String annotationSetName = annotationSet.getString(AnnotationSetParams.NAME.key());

                // Build a query to look for the particular annotations
                Bson bsonQuery = Filters.and(
                        Filters.eq(PRIVATE_ID, entityId),
                        Filters.eq(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSetName),
                        Filters.eq(AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS_NAME.key(), oldName)
                );

                // And extract those annotations from the annotation set
                Bson update = Updates.pull(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + AnnotationSetParams.ANNOTATIONS.key(),
                        Filters.eq(AnnotationSetParams.NAME.key(), oldName));

                QueryResult<UpdateResult> queryResult = getCollection().update(bsonQuery, update, null);

                if (queryResult.first().getModifiedCount() != 1) {
                    throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - AnnotationSet {name: "
                            + annotationSet.getString(AnnotationSetParams.NAME.key()) + "} - An unexpected error happened when "
                            + "extracting the annotation " + oldName + ". Please, report this error to the OpenCGA developers.");
                }

                // Obtain the value of the annotation
                Object value = ((Document) annotationSet.get(AnnotationSetParams.ANNOTATIONS.key())).get(AnnotationSetParams.VALUE.key());

                // Create a new annotation with the new id and the former value
                Annotation annotation = new Annotation(newName, value);

                bsonQuery = Filters.and(
                        Filters.eq(PRIVATE_ID, entityId),
                        Filters.eq(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSetName)
                );

                // Push the again the annotation with the new name
                update = Updates.push(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + AnnotationSetParams.ANNOTATIONS.key(),
                        MongoDBUtils.getMongoDBDocument(annotation, "Annotation"));
                queryResult = getCollection().update(bsonQuery, update, null);

                if (queryResult.first().getModifiedCount() != 1) {
                    throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - AnnotationSet {name: "
                            + annotationSetName + "} - A critical error happened when trying to rename the annotation " + oldName
                            + ". Please, report this error to the OpenCGA developers.");
                }
                renamedAnnotations += 1;

            }
        }
        return endQuery("Rename annotation name", startTime, Collections.singletonList(renamedAnnotations));
    }

    public QueryResult<Long> removeAnnotationField(long variableSetId, String fieldId) throws CatalogDBException {
        long startTime = startQuery();
        long removedAnnotations = 0;
        List<Document> aggregateResult = getAnnotationDocuments(variableSetId, fieldId);

        if (aggregateResult.size() > 0) {
            // Each document will be a cohort, sample or individual
            for (Document entity : aggregateResult) {
                Object entityId = entity.get(AnnotationSetParams.ID.key());
                Document annotationSet = ((Document) entity.get(AnnotationSetParams.ANNOTATION_SETS.key()));

                String annotationSetName = annotationSet.getString(AnnotationSetParams.NAME.key());

                // Build a query to look for the particular annotations
                Bson bsonQuery = Filters.and(
                        Filters.eq(PRIVATE_ID, entityId),
                        Filters.eq(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSetName),
                        Filters.eq(AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS_NAME.key(), fieldId)
                );

                // Extract those annotations
                Bson update = Updates.pull(AnnotationSetParams.ANNOTATION_SETS.key() + ".$." + AnnotationSetParams.ANNOTATIONS.key(),
                        Filters.eq(AnnotationSetParams.NAME.key(), fieldId));
                QueryResult<UpdateResult> queryResult = getCollection().update(bsonQuery, update, null);
                if (queryResult.first().getModifiedCount() != 1) {
                    throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - AnnotationSet {name: "
                            + annotationSetName + "} - An unexpected error happened when extracting the annotation " + fieldId
                            + ". Please, report this error to the OpenCGA developers.");
                }

                removedAnnotations += 1;

            }
        }
        return endQuery("Remove annotation", startTime, Collections.singletonList(removedAnnotations));
    }

    private List<Document> getAnnotationDocuments(long variableSetId, String oldName) {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(
                Filters.elemMatch(AnnotationSetParams.ANNOTATION_SETS.key(),
                        Filters.eq(AnnotationSetParams.VARIABLE_SET_ID.key(), variableSetId))));
        aggregation.add(Aggregates.project(Projections.include(AnnotationSetParams.ANNOTATION_SETS.key(), AnnotationSetParams.ID.key())));
        aggregation.add(Aggregates.unwind("$" + AnnotationSetParams.ANNOTATION_SETS.key()));
        aggregation.add(Aggregates.match(Filters.eq(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), variableSetId)));
        aggregation.add(Aggregates.unwind("$" + AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS.key()));
        aggregation.add(Aggregates.match(
                Filters.eq(AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS_NAME.key(), oldName)));

        return getCollection().aggregate(aggregation, new QueryOptions()).getResult();
    }

    public QueryResult<VariableSummary> getAnnotationSummary(long variableSetId) throws CatalogDBException {
        long startTime = startQuery();

        List<Bson> aggregation = new ArrayList<>(6);
        aggregation.add(new Document("$project", new Document(AnnotationSetParams.ANNOTATION_SETS.key(), 1)));
        aggregation.add(new Document("$unwind", "$" + AnnotationSetParams.ANNOTATION_SETS.key()));
        aggregation.add(new Document("$unwind", "$" + AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS.key()));
        // TODO: Include annotations of type object
        // At the moment, we are excluding the annotations of type Object.
        aggregation.add(new Document("$match",
                new Document(AnnotationSetParams.ANNOTATION_SETS_VARIABLE_SET_ID.key(), variableSetId)
                    .append(AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS_VALUE.key(),
                            new Document("$not", new Document("$type", "object"))
                    )
                )
        );
        aggregation.add(new Document("$group",
                new Document(
                        "_id", new Document("name", "$" + AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS_NAME.key())
                            .append("value", "$" + AnnotationSetParams.ANNOTATION_SETS_ANNOTATIONS_VALUE.key()))
                        .append("count", new Document("$sum", 1))
                )
        );
        aggregation.add(new Document("$sort", new Document("_id.name", -1).append("count", -1)));

        List<Document> result = getCollection().aggregate(aggregation, new QueryOptions()).getResult();

        List<VariableSummary> variableSummaryList = new ArrayList<>();

        List<FeatureCount> featureCountList = null;
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

}
