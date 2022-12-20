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
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.AbstractDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.getMongoDBDocument;

/**
 * Created by jacobo on 12/09/14.
 */
public class MongoDBAdaptor extends AbstractDBAdaptor {

    public static final String PRIVATE_UID = "uid";
    public static final String PRIVATE_UUID = "uuid";
    static final String PRIVATE_MONGO_ID = "_id";
    static final String ID = "id";
    static final String PRIVATE_FQN = "fqn";
    static final String PRIVATE_PROJECT = "_project";
    static final String PRIVATE_PROJECT_ID = PRIVATE_PROJECT + '.' + ID;
    static final String PRIVATE_PROJECT_UID = PRIVATE_PROJECT + '.' + PRIVATE_UID;
    static final String PRIVATE_PROJECT_UUID = PRIVATE_PROJECT + '.' + PRIVATE_UUID;
    static final String PRIVATE_OWNER_ID = "_ownerId";
    public static final String PRIVATE_STUDY_UID = "studyUid";
    public static final String VERSION = "version";

    static final String FILTER_ROUTE_STUDIES = "projects.studies.";
    static final String FILTER_ROUTE_COHORTS = "projects.studies.cohorts.";
    static final String FILTER_ROUTE_INDIVIDUALS = "projects.studies.individuals.";
    static final String FILTER_ROUTE_SAMPLES = "projects.studies.samples.";
    static final String FILTER_ROUTE_FILES = "projects.studies.files.";
    static final String FILTER_ROUTE_JOBS = "projects.studies.jobs.";

    public static final String LAST_OF_VERSION = "_lastOfVersion";
    static final String RELEASE_FROM_VERSION = "_releaseFromVersion";
    static final String LAST_OF_RELEASE = "_lastOfRelease";
    static final String PRIVATE_CREATION_DATE = "_creationDate";
    static final String PRIVATE_MODIFICATION_DATE = "_modificationDate";
    static final String PERMISSION_RULES_APPLIED = "_permissionRulesApplied";
    static final String INTERNAL_LAST_MODIFIED = "internal.lastModified";

    static final String INTERNAL_DELIMITER = "__";

    public static final String NATIVE_QUERY = "nativeQuery";

    protected MongoDBAdaptorFactory dbAdaptorFactory;
    protected Configuration configuration;

    protected static final QueryOptions EXCLUDE_MONGO_ID = new QueryOptions(QueryOptions.EXCLUDE, PRIVATE_MONGO_ID);

    public MongoDBAdaptor(Configuration configuration, Logger logger) {
        super(logger);
        this.configuration = configuration;
    }

    public interface TransactionBodyWithException<T> {
        T execute(ClientSession session) throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;
    }

    protected <T> T runTransaction(TransactionBodyWithException<T> body)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(body, null);
    }

    protected <T> T runTransaction(TransactionBodyWithException<T> body, Consumer<CatalogException> onException)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        ClientSession session = dbAdaptorFactory.getMongoDataStore().startSession();
        try {
            return session.withTransaction(() -> {
                try {
                    return body.execute(session);
                } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                    throw new CatalogDBRuntimeException(e);
                }
            });
        } catch (CatalogDBRuntimeException e) {
            if (e.getCause() instanceof CatalogDBException) {
                CatalogDBException cause = (CatalogDBException) e.getCause();
                if (onException != null) {
                    onException.accept(cause);
                }
                throw cause;
            } else if (e.getCause() instanceof CatalogAuthorizationException) {
                CatalogAuthorizationException cause = (CatalogAuthorizationException) e.getCause();
                if (onException != null) {
                    onException.accept(cause);
                }
                throw cause;
            } else if (e.getCause() instanceof CatalogParameterException) {
                CatalogParameterException cause = (CatalogParameterException) e.getCause();
                if (onException != null) {
                    onException.accept(cause);
                }
                throw cause;
            } else {
                throw e;
            }
        } finally {
            session.close();
        }
    }

    /**
     * Check if user is performing a query over versioned data or deleted data to provide the collection containing it.
     *
     * @param query Query.
     * @param collection Collection containing just the last "active" data.
     * @param archiveCollection Collection containing the whole archive of data.
     * @param deleteCollection Collection containing the whole archive of data.
     * @return The collection containing the data the user is querying.
     */
    protected MongoDBCollection getQueryCollection(Query query, MongoDBCollection collection, MongoDBCollection archiveCollection,
                                                   MongoDBCollection deleteCollection) {
        if (query.getBoolean(ParamConstants.DELETED_PARAM, false)) {
            return deleteCollection;
        }
        if (query.getBoolean(Constants.ALL_VERSIONS, false) || query.containsKey(VERSION)
                || query.containsKey(ParamConstants.SNAPSHOT_PARAM)) {
            return archiveCollection;
        }
        return collection;
    }

    protected long getNewUid() {
//        return CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);
        return dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();
    }

    protected long getNewUid(ClientSession clientSession) {
//        return CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);
        return dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId(clientSession);
    }

    @Deprecated
    protected void addIntegerOrQuery(String mongoDbField, String queryParam, Query query, List<Bson> andBsonList) {
        addQueryFilter(mongoDbField, queryParam, query, QueryParam.Type.INTEGER, MongoDBQueryUtils.ComparisonOperator.EQUALS,
                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
    }

    @Deprecated
    protected void addStringOrQuery(String mongoDbField, String queryParam, Query query, List<Bson> andBsonList) {
        addQueryFilter(mongoDbField, queryParam, query, QueryParam.Type.TEXT, MongoDBQueryUtils.ComparisonOperator.EQUALS,
                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
    }


    @Deprecated
    protected void addStringOrQuery(String mongoDbField, String queryParam, Query query, MongoDBQueryUtils.ComparisonOperator
            comparisonOperator, List<Bson> andBsonList) {
        addQueryFilter(mongoDbField, queryParam, query, QueryParam.Type.TEXT, comparisonOperator,
                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
    }

    /**
     * It will add a filter to andBsonList based on the query object. The operator will always be an EQUAL.
     *
     * @param mongoDbField The field used in the mongoDB.
     * @param queryParam   The key by which the parameter is stored in the query. Normally, it will be the same as in the data model,
     *                     although it might be some exceptions.
     * @param query        The object containing the key:values of the query.
     * @param paramType    The type of the object to be looked up. See {@link QueryParam}.
     * @param andBsonList  The list where created filter will be added to.
     */
    protected void addOrQuery(String mongoDbField, String queryParam, Query query, QueryParam.Type paramType, List<Bson> andBsonList) {
        addQueryFilter(mongoDbField, queryParam, query, paramType, MongoDBQueryUtils.ComparisonOperator.IN,
                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
    }

    /**
     * It will check for the proper comparator based on the query value and create the correct query filter.
     * It could be a regular expression, >, < ... or a simple equals.
     *
     * @param mongoDbField The field used in the mongoDB.
     * @param queryParam   The key by which the parameter is stored in the query. Normally, it will be the same as in the data model,
     *                     although it might be some exceptions.
     * @param query        The object containing the key:values of the query.
     * @param paramType    The type of the object to be looked up. See {@link QueryParam}.
     * @param andBsonList  The list where created filter will be added to.
     */
    protected void addAutoOrQuery(String mongoDbField, String queryParam, Query query, QueryParam.Type paramType, List<Bson> andBsonList) {
        if (query != null && query.getString(queryParam) != null) {
            Bson filter;
            if (paramType == QueryParam.Type.STRING || paramType == QueryParam.Type.TEXT || paramType == QueryParam.Type.TEXT_ARRAY) {
                filter = MongoDBQueryUtils.createStringFilter(mongoDbField, queryParam, query,
                        ObjectMap.COMMA_SEPARATED_LIST_SPLIT_PATTERN);
            } else {
                filter = MongoDBQueryUtils.createAutoFilter(mongoDbField, queryParam, query, paramType);
            }
            if (filter != null) {
                andBsonList.add(filter);
            }
        }
    }

    protected void addQueryFilter(String mongoDbField, String queryParam, Query query, QueryParam.Type paramType,
                                  MongoDBQueryUtils.ComparisonOperator comparisonOperator, MongoDBQueryUtils.LogicalOperator operator,
                                  List<Bson> andBsonList) {
        if (query != null && query.getString(queryParam) != null) {
            Bson filter = MongoDBQueryUtils.createFilter(mongoDbField, queryParam, query, paramType, comparisonOperator, operator);
            if (filter != null) {
                andBsonList.add(filter);
            }
        }
    }

    protected OpenCGAResult rank(MongoDBCollection collection, Bson query, String groupByField, String idField, int numResults,
                                 boolean asc) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new OpenCGAResult();
        }

        if (groupByField.contains(",")) {
            // call to multiple rank if commas are present
            return rank(collection, query, Arrays.asList(groupByField.split(",")), idField, numResults, asc);
        } else {
            Bson match = Aggregates.match(query);
            Bson project = Aggregates.project(Projections.include(groupByField, idField));
            Bson group = Aggregates.group("$" + groupByField, Accumulators.sum("count", 1));
            Bson sort;
            if (asc) {
                sort = Aggregates.sort(Sorts.ascending("count"));
            } else {
                sort = Aggregates.sort(Sorts.descending("count"));
            }
            Bson limit = Aggregates.limit(numResults);

            return new OpenCGAResult(collection.aggregate(Arrays.asList(match, project, group, sort, limit), new QueryOptions()));
        }
    }

    protected OpenCGAResult rank(MongoDBCollection collection, Bson query, List<String> groupByField, String idField, int numResults,
                                 boolean asc) {

        if (groupByField == null || groupByField.isEmpty()) {
            return new OpenCGAResult();
        }

        if (groupByField.size() == 1) {
            // if only one field then we call to simple rank
            return rank(collection, query, groupByField.get(0), idField, numResults, asc);
        } else {
            Bson match = Aggregates.match(query);

            // add all group-by fields to the projection together with the aggregation field name
            List<String> groupByFields = new ArrayList<>(groupByField);
            groupByFields.add(idField);
            Bson project = Aggregates.project(Projections.include(groupByFields));

            // _id document creation to have the multiple id
            Document id = new Document();
            for (String s : groupByField) {
                id.append(s, "$" + s);
            }
            Bson group = Aggregates.group(id, Accumulators.sum("count", 1));
            Bson sort;
            if (asc) {
                sort = Aggregates.sort(Sorts.ascending("count"));
            } else {
                sort = Aggregates.sort(Sorts.descending("count"));
            }
            Bson limit = Aggregates.limit(numResults);

            return new OpenCGAResult(collection.aggregate(Arrays.asList(match, project, group, sort, limit), new QueryOptions()));
        }
    }

    protected OpenCGAResult groupBy(MongoDBCollection collection, Bson query, String groupByField, String idField, QueryOptions options) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new OpenCGAResult();
        }

        if (groupByField.contains(",")) {
            // call to multiple groupBy if commas are present
            return groupBy(collection, query, Arrays.asList(groupByField.split(",")), idField, options);
        } else {
            return groupBy(collection, query, Arrays.asList(groupByField), idField, options);
        }
    }

    protected OpenCGAResult groupBy(MongoDBCollection collection, Bson query, List<String> groupByField, String idField,
                                    QueryOptions options) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new OpenCGAResult();
        }

        List<String> groupByFields = new ArrayList<>(groupByField);
        Bson match = Aggregates.match(query);

        // add all group-by fields to the projection together with the aggregation field name
        List<String> includeGroupByFields = new ArrayList<>(groupByFields);
        includeGroupByFields.add(idField);
        Document projection = createDateProjection(includeGroupByFields, groupByFields);
        Document annotationDocument = createAnnotationProjectionForGroupBy(includeGroupByFields);
        projection.putAll(annotationDocument);

        for (String field : includeGroupByFields) {
            // Include the parameters from the includeGroupByFields list
            projection.append(field, 1);
        }
        Bson project = Aggregates.project(projection);

        // _id document creation to have the multiple id
        Document id = new Document();
        for (String s : groupByFields) {
            id.append(s.replace(".", GenericDocumentComplexConverter.TO_REPLACE_DOTS), "$" + s);
        }
        Bson group;
        if (options.getBoolean(QueryOptions.COUNT, false)) {
            group = Aggregates.group(id, Accumulators.sum(QueryOptions.COUNT, 1));
        } else {
            group = Aggregates.group(id, Accumulators.addToSet("items", "$" + idField));
        }
        DataResult<Document> aggregate = collection.aggregate(Arrays.asList(match, project, group), options);
        for (String s : groupByField) {
            if (s.contains(".")) {
                aggregate.getResults().stream().map(d -> d.get(PRIVATE_MONGO_ID, Document.class)).forEach(d -> {
                    Object o = d.remove(s.replace(".", GenericDocumentComplexConverter.TO_REPLACE_DOTS));
                    d.put(s, o);
                });
            }
        }
        return new OpenCGAResult<>(aggregate);
    }

    /**
     * Filter QueryOptions object to ensure the keys provided are always included.
     *
     * @param options QueryOptions object.
     * @param keys    Keys that always need to be included in the response.
     * @return A new QueryOptions object containing the mandatory fields.
     */
    protected QueryOptions filterQueryOptions(QueryOptions options, List<String> keys) {
        if (options == null) {
            return null;
        }

        QueryOptions queryOptions = new QueryOptions(options);

        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            Set<String> includeList = new HashSet<>(queryOptions.getAsStringList(QueryOptions.INCLUDE));
            includeList.addAll(keys);
            queryOptions.put(QueryOptions.INCLUDE, new ArrayList<>(includeList));
        }
        if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            Set<String> excludeList = new HashSet<>(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
            excludeList.removeAll(keys);
            queryOptions.put(QueryOptions.EXCLUDE, new ArrayList<>(excludeList));
        }

        return queryOptions;
    }


    /**
     * Create a date projection if included in the includeGroupByFields, removes the date fields from includeGroupByFields and
     * add them to groupByFields if not there.
     * Only for groupBy methods.
     *
     * @param includeGroupByFields List containing the fields to be included in the projection.
     * @param groupByFields        List containing the fields by which the group by will be done.
     */
    private Document createDateProjection(List<String> includeGroupByFields, List<String> groupByFields) {
        Document dateProjection = new Document();
        Document year = new Document("$year", "$" + PRIVATE_CREATION_DATE);
        Document month = new Document("$month", "$" + PRIVATE_CREATION_DATE);
        Document day = new Document("$dayOfMonth", "$" + PRIVATE_CREATION_DATE);

        if (includeGroupByFields.contains("day")) {
            dateProjection.append("day", day).append("month", month).append("year", year);
            includeGroupByFields.remove("day");
            if (!includeGroupByFields.remove("month")) {
                groupByFields.add("month");
            }
            if (!includeGroupByFields.remove("year")) {
                groupByFields.add("year");
            }

        } else if (includeGroupByFields.contains("month")) {
            dateProjection.append("month", month).append("year", year);
            includeGroupByFields.remove("month");
            if (!includeGroupByFields.remove("year")) {
                groupByFields.add("year");
            }
        } else if (includeGroupByFields.contains("year")) {
            dateProjection.append("year", year);
            includeGroupByFields.remove("year");
        }

        return dateProjection;
    }

    /**
     * Fixes the annotation ids provided by the user to create a proper groupBy by any annotation field provided.
     *
     * @param includeGroupByFields List containing the fields to be included in the projection.
     */
    private Document createAnnotationProjectionForGroupBy(List<String> includeGroupByFields) {
        Document document = new Document();

        Iterator<String> iterator = includeGroupByFields.iterator();
        while (iterator.hasNext()) {
            String field = iterator.next();

            if (field.startsWith(Constants.ANNOTATION)) {
                String replacedField = field
                        .replace(Constants.ANNOTATION + ":", "")
                        .replace(":", INTERNAL_DELIMITER)
                        .replace(".", INTERNAL_DELIMITER);
                iterator.remove();

                document.put(field, "$" + AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SETS.key() + "." + replacedField);
            }
        }

        return document;
    }

    /**
     * Extract a new QueryOptions object containing only the include/exclude of another nested object.
     * Example: Let's say a user is querying the user collection adding include: projects.studies.fqn
     * If we need to perform a different query in the study collection, we will want to obtain a new QueryOptions object containing:
     * include: fqn
     * <p>
     * For that scenario, the `key` value would be "projects.studies"
     *
     * @param options Original QueryOptions object.
     * @param key     Nested key by which to extract the new options.
     * @return new QueryOptions object.
     */
    protected QueryOptions extractNestedOptions(QueryOptions options, String key) {
        QueryOptions studyOptions = new QueryOptions();

        List<String> includeList = options.getAsStringList(QueryOptions.INCLUDE);
        List<String> excludeList = options.getAsStringList(QueryOptions.EXCLUDE);

        String projectionKey = key.endsWith(".") ? key : key + ".";

        if (!includeList.isEmpty()) {
            List<String> studyIncludeList = new ArrayList<>();
            for (String includeKey : includeList) {
                if (includeKey.startsWith(projectionKey)) {
                    studyIncludeList.add(includeKey.replace(projectionKey, ""));
                }
            }
            if (!studyIncludeList.isEmpty()) {
                studyOptions.put(QueryOptions.INCLUDE, studyIncludeList);
            }
        } else if (!excludeList.isEmpty()) {
            List<String> studyExcludeList = new ArrayList<>();
            for (String excludeKey : excludeList) {
                if (excludeKey.startsWith(projectionKey)) {
                    studyExcludeList.add(excludeKey.replace(projectionKey, ""));
                }
            }
            if (!studyExcludeList.isEmpty()) {
                studyOptions.put(QueryOptions.EXCLUDE, studyExcludeList);
            }
        }

        return studyOptions;
    }

    /**
     * Removes any other entity projections made. This method should be called by any entity containing inner entities:
     * Family -> Individual; Individual -> Sample; File -> Sample; Cohort -> Sample
     *
     * @param options       current query options object.
     * @param projectionKey Projection key to be removed from the query options.
     * @return new QueryOptions after removing the inner projectionKey projections.
     */
    protected QueryOptions removeInnerProjections(QueryOptions options, String projectionKey) {
        QueryOptions queryOptions = ParamUtils.defaultObject(options, QueryOptions::new);

        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            List<String> includeList = queryOptions.getAsStringList(QueryOptions.INCLUDE);
            List<String> newInclude = new ArrayList<>(includeList.size());
            boolean projectionKeyExcluded = false;
            for (String include : includeList) {
                if (!include.startsWith(projectionKey + ".")) {
                    newInclude.add(include);
                } else {
                    projectionKeyExcluded = true;
                }
            }
            if (newInclude.isEmpty()) {
                queryOptions.put(QueryOptions.INCLUDE, Arrays.asList(ID, projectionKey));
            } else {
                if (projectionKeyExcluded) {
                    newInclude.add(projectionKey);
                }
                queryOptions.put(QueryOptions.INCLUDE, newInclude);
            }
        }
        if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            List<String> excludeList = queryOptions.getAsStringList(QueryOptions.EXCLUDE);
            List<String> newExclude = new ArrayList<>(excludeList.size());
            for (String exclude : excludeList) {
                if (!exclude.startsWith(projectionKey + ".")) {
                    newExclude.add(exclude);
                }
            }
            if (newExclude.isEmpty()) {
                queryOptions.remove(QueryOptions.EXCLUDE);
            } else {
                queryOptions.put(QueryOptions.EXCLUDE, newExclude);
            }
        }

        return queryOptions;
    }

    /**
     * Change the projection key given by the user by the real one used internally.
     *
     * @param options           current query options object.
     * @param userProjectionKey Projection key provided by the user.
     * @param realProjectionKey Real projection key we need to have..
     * @return new QueryOptions after changing the projection key.
     */
    protected QueryOptions changeProjectionKey(QueryOptions options, String userProjectionKey, String realProjectionKey) {
        QueryOptions queryOptions;
        if (options == null) {
            queryOptions = new QueryOptions();
        } else {
            queryOptions = new QueryOptions(options);
        }

        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            List<String> includeList = queryOptions.getAsStringList(QueryOptions.INCLUDE);
            if (includeList.contains(userProjectionKey)) {
                includeList.remove(userProjectionKey);
                includeList.add(realProjectionKey);
                queryOptions.put(QueryOptions.INCLUDE, includeList);
            }
        }
        if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            List<String> excludeList = queryOptions.getAsStringList(QueryOptions.EXCLUDE);
            if (excludeList.contains(userProjectionKey)) {
                excludeList.remove(userProjectionKey);
                excludeList.add(realProjectionKey);
                queryOptions.put(QueryOptions.EXCLUDE, excludeList);
            }
        }

        return queryOptions;
    }

    /**
     * Generate Mongo operation to perform a nested array replacement.
     *
     * @param entryList  Object list containing the elements to be replaced.
     * @param document   UpdateDocument to be filled in with the corresponding mongo operation.
     * @param idFunction function to retrieve the identifier of each entry element to be replaced.
     * @param queryKey   mongo key by which we will perform the replacement operation.
     * @param <T>        Type of object.
     * @throws CatalogDBException if there is any issue converting the object to the Document class.
     */
    protected <T> void filterReplaceParams(List<T> entryList, MongoDBAdaptor.UpdateDocument document, Function<T, String> idFunction,
                                           String queryKey) throws CatalogDBException {
        if (entryList == null) {
            return;
        }

        String updateKey = queryKey.substring(0, queryKey.lastIndexOf("."));
        for (T entry : entryList) {
            String id = idFunction.apply(entry);
            Document entryDocument = new Document("$set", new Document(updateKey + ".$", getMongoDBDocument(entry, "")));
            document.addNestedUpdateDocument(new MongoDBAdaptor.NestedArrayUpdateDocument(new Query(queryKey, id), entryDocument));
        }
    }

    protected OpenCGAResult unmarkPermissionRule(MongoDBCollection collection, long studyId, String permissionRuleId) {
        Bson query = new Document()
                .append(PRIVATE_STUDY_UID, studyId)
                .append(PERMISSION_RULES_APPLIED, permissionRuleId);
        Bson update = Updates.pull(PERMISSION_RULES_APPLIED, permissionRuleId);

        return new OpenCGAResult(collection.update(query, update, new QueryOptions("multi", true)));
    }

    protected Document getStudyDocument(ClientSession clientSession, long studyUid) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), studyUid);
        DataResult<Document> dataResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery,
                QueryOptions.empty());
        if (dataResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + studyUid + " not found");
        }
        return dataResult.first();
    }

    public class NestedArrayUpdateDocument {
        private Query query;
        private Document set;

        public NestedArrayUpdateDocument(Query query, Document set) {
            this.query = query;
            this.set = set;
        }

        public Query getQuery() {
            return query;
        }

        public NestedArrayUpdateDocument setQuery(Query query) {
            this.query = query;
            return this;
        }

        public Document getSet() {
            return set;
        }

        public NestedArrayUpdateDocument setSet(Document set) {
            this.set = set;
            return this;
        }
    }

    public static class UpdateDocument {
        private Document set;
        private Document addToSet;
        private Document push;
        private Document pull;
        private Document pullAll;
        private List<String> unset;
        private List<NestedArrayUpdateDocument> nestedUpdateList;

        private ObjectMap attributes;

        public UpdateDocument() {
            this.set = new Document();
            this.addToSet = new Document();
            this.push = new Document();
            this.pull = new Document();
            this.pullAll = new Document();
            this.unset = new LinkedList<>();
            this.nestedUpdateList = new LinkedList<>();
            this.attributes = new ObjectMap();
        }

        public Document toFinalUpdateDocument() {
            Document update = new Document();
            if (!set.isEmpty()) {
                update.put("$set", set);
            }
            if (!addToSet.isEmpty()) {
                for (Map.Entry<String, Object> entry : addToSet.entrySet()) {
                    if (entry.getValue() instanceof Collection) {
                        // We need to add all the elements of the array
                        entry.setValue(new Document("$each", entry.getValue()));
                    }
                }
                update.put("$addToSet", addToSet);
            }
            if (!push.isEmpty()) {
                for (Map.Entry<String, Object> entry : push.entrySet()) {
                    if (entry.getValue() instanceof Collection) {
                        // We need to add all the elements of the array
                        entry.setValue(new Document("$each", entry.getValue()));
                    }
                }
                update.put("$push", push);
            }
            if (!pull.isEmpty()) {
                for (Map.Entry<String, Object> entry : pull.entrySet()) {
                    if (entry.getValue() instanceof Collection) {
                        // We need to pull all the elements of the array
                        entry.setValue(new Document("$or", entry.getValue()));
                    }
                }
                update.put("$pull", pull);
            }
            if (!pullAll.isEmpty()) {
                update.put("$pullAll", pullAll);
            }
            if (!unset.isEmpty()) {
                Document unsetDocument = new Document();
                for (String field : unset) {
                    unsetDocument.put(field, "");
                }
                update.put("$unset", unsetDocument);
            }

            return update;
        }

        public Document getSet() {
            return set;
        }

        public UpdateDocument setSet(Document set) {
            this.set = set;
            return this;
        }

        public Document getAddToSet() {
            return addToSet;
        }

        public UpdateDocument setAddToSet(Document addToSet) {
            this.addToSet = addToSet;
            return this;
        }

        public Document getPush() {
            return push;
        }

        public UpdateDocument setPush(Document push) {
            this.push = push;
            return this;
        }

        public Document getPull() {
            return pull;
        }

        public UpdateDocument setPull(Document pull) {
            this.pull = pull;
            return this;
        }

        public Document getPullAll() {
            return pullAll;
        }

        public UpdateDocument setPullAll(Document pullAll) {
            this.pullAll = pullAll;
            return this;
        }

        public List<String> getUnset() {
            return unset;
        }

        public UpdateDocument setUnset(List<String> unset) {
            this.unset = unset;
            return this;
        }

        public List<NestedArrayUpdateDocument> getNestedUpdateList() {
            return nestedUpdateList;
        }

        public UpdateDocument addNestedUpdateDocument(NestedArrayUpdateDocument nestedUpdateDocument) {
            this.nestedUpdateList.add(nestedUpdateDocument);
            return this;
        }

        public ObjectMap getAttributes() {
            return attributes;
        }

        public UpdateDocument setAttributes(ObjectMap attributes) {
            this.attributes = attributes;
            return this;
        }
    }

}
