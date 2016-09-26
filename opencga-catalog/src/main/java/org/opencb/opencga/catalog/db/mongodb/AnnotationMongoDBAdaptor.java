package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.collections.map.LinkedMap;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.mongodb.converters.GenericConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Annotable;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.summaries.FeatureCount;
import org.opencb.opencga.catalog.models.summaries.VariableSummary;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 07/07/16.
 */
abstract class AnnotationMongoDBAdaptor extends MongoDBAdaptor {

    AnnotationMongoDBAdaptor(Logger logger) {
        super(logger);
    }

    protected abstract GenericConverter<? extends Annotable, Document> getConverter();

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

    public QueryResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<? extends Annotable> aggregate = commonGetAnnotationSet(id, annotationSetName);

        List<AnnotationSet> annotationSets = new ArrayList<>(aggregate.getNumResults());
        for (Annotable annotable : aggregate.getResult()) {
            annotationSets.add((AnnotationSet) annotable.getAnnotationSets().get(0));
        }

        return endQuery("Get annotation set", startTime, annotationSets);
    }

    public QueryResult<ObjectMap> getAnnotationSetAsMap(long id, @Nullable String annotationSetName) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<? extends Annotable> aggregate = commonGetAnnotationSet(id, annotationSetName);

        List<ObjectMap> annotationSets = new ArrayList<>(aggregate.getNumResults());
        for (Annotable annotable : aggregate.getResult()) {
            annotationSets.add((ObjectMap) annotable.getAnnotationSetAsMap().get(0));
        }

        return endQuery("Get annotation set", startTime, annotationSets);
    }

    private QueryResult<? extends Annotable> commonGetAnnotationSet(long id, @Nullable String annotationSetName) {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, id)));
        aggregation.add(Aggregates.project(Projections.include(AnnotationSetParams.ID.key(), AnnotationSetParams.ANNOTATION_SETS.key())));
        aggregation.add(Aggregates.unwind("$" + AnnotationSetParams.ANNOTATION_SETS.key()));

        List<Bson> filters = new ArrayList<>();
        if (annotationSetName != null && !annotationSetName.isEmpty()) {
            filters.add(Filters.eq(AnnotationSetParams.ANNOTATION_SETS_NAME.key(), annotationSetName));
        }

        if (filters.size() > 0) {
            Bson filter = filters.size() == 1 ? filters.get(0) : Filters.and(filters);
            aggregation.add(Aggregates.match(filter));
        }

        for (Bson bson : aggregation) {
            logger.debug("Get annotation: {}", bson.toBsonDocument(Document.class, com.mongodb.MongoClient.getDefaultCodecRegistry()));
        }

        return getCollection().aggregate(aggregation, getConverter(), null);
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

        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("The annotation set could not be updated.");
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
