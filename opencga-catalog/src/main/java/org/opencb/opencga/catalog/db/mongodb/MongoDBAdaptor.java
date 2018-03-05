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

import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.AbstractDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Sample;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 12/09/14.
 */
public class MongoDBAdaptor extends AbstractDBAdaptor {

    static final String PRIVATE_ID = "id";
    static final String PRIVATE_PROJECT_ID = "_projectId";
    static final String PRIVATE_OWNER_ID = "_ownerId";
    static final String PRIVATE_STUDY_ID = "_studyId";

    static final String FILTER_ROUTE_PROJECTS = "projects.";
    static final String FILTER_ROUTE_STUDIES = "projects.studies.";
    static final String FILTER_ROUTE_COHORTS = "projects.studies.cohorts.";
    static final String FILTER_ROUTE_DATASETS = "projects.studies.datasets.";
    static final String FILTER_ROUTE_INDIVIDUALS = "projects.studies.individuals.";
    static final String FILTER_ROUTE_SAMPLES = "projects.studies.samples.";
    static final String FILTER_ROUTE_FILES = "projects.studies.files.";
    static final String FILTER_ROUTE_JOBS = "projects.studies.jobs.";
    static final String FILTER_ROUTE_PANELS = "projects.studies.panels.";

    static final String LAST_OF_VERSION = "_lastOfVersion";
    static final String RELEASE_FROM_VERSION = "_releaseFromVersion";
    static final String LAST_OF_RELEASE = "_lastOfRelease";
    static final String PRIVATE_CREATION_DATE = "_creationDate";
    static final String PERMISSION_RULES_APPLIED = "_permissionRulesApplied";

    static final String INTERNAL_DELIMITER = "__";

    protected MongoDBAdaptorFactory dbAdaptorFactory;

    public MongoDBAdaptor(Logger logger) {
        super(logger);
    }

    protected long getNewId() {
//        return CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);
        return dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();
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
     * @param mongoDbField The field used in the mongoDB.
     * @param queryParam The key by which the parameter is stored in the query. Normally, it will be the same as in the data model,
     *                   although it might be some exceptions.
     * @param query The object containing the key:values of the query.
     * @param paramType The type of the object to be looked up. See {@link QueryParam}.
     * @param andBsonList The list where created filter will be added to.
     */
    protected void addOrQuery(String mongoDbField, String queryParam, Query query, QueryParam.Type paramType, List<Bson> andBsonList) {
        addQueryFilter(mongoDbField, queryParam, query, paramType, MongoDBQueryUtils.ComparisonOperator.EQUALS,
                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
    }

    /**
     * It will check for the proper comparator based on the query value and create the correct query filter.
     * It could be a regular expression, >, < ... or a simple equals.
     * @param mongoDbField The field used in the mongoDB.
     * @param queryParam The key by which the parameter is stored in the query. Normally, it will be the same as in the data model,
     *                   although it might be some exceptions.
     * @param query The object containing the key:values of the query.
     * @param paramType The type of the object to be looked up. See {@link QueryParam}.
     * @param andBsonList The list where created filter will be added to.
     */
    protected void addAutoOrQuery(String mongoDbField, String queryParam, Query query, QueryParam.Type paramType, List<Bson> andBsonList) {
        if (query != null && query.getString(queryParam) != null) {
            Bson filter = MongoDBQueryUtils.createAutoFilter(mongoDbField, queryParam, query, paramType);
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

    // Auxiliar methods used in family/get and clinicalAnalysis/get to retrieve the whole referenced documents
    protected Individual getIndividual(Individual individual) {
        Individual retIndividual = individual;
        if (individual != null && individual.getId() > 0) {
            // Fetch individual information
            QueryResult<Individual> individualQueryResult = null;
            try {
                individualQueryResult = dbAdaptorFactory.getCatalogIndividualDBAdaptor().get(individual.getId(),
                        QueryOptions.empty());
            } catch (CatalogDBException e) {
                logger.error(e.getMessage(), e);
            }
            if (individualQueryResult != null && individualQueryResult.getNumResults() == 1) {
                retIndividual = individualQueryResult.first();

                // Fetch samples from individual
                List<Long> samples = individual.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
                Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), samples);
                try {
                    QueryResult<Sample> sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, QueryOptions.empty());
                    retIndividual.setSamples(sampleQueryResult.getResult());
                } catch (CatalogDBException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return retIndividual;
    }

    protected Sample getSample(Sample sample) {
        Sample retSample = sample;
        if (sample != null && sample.getId() > 0) {
            QueryResult<Sample> sampleQueryResult = null;
            try {
                sampleQueryResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(sample.getId(), QueryOptions.empty());
            } catch (CatalogDBException e) {
                logger.error(e.getMessage(), e);
            }
            if (sampleQueryResult != null && sampleQueryResult.getNumResults() == 1) {
                retSample = sampleQueryResult.first();
            }
        }
        return retSample;
    }

    protected Family getFamily(Family family) {
        Family retFamily = family;
        if (family != null && family.getId() > 0) {
            QueryResult<Family> familyQueryResult = null;
            try {
                familyQueryResult = dbAdaptorFactory.getCatalogFamilyDBAdaptor().get(family.getId(), QueryOptions.empty());
            } catch (CatalogDBException e) {
                logger.error(e.getMessage(), e);
            }
            if (familyQueryResult != null && familyQueryResult.getNumResults() == 1) {
                retFamily = familyQueryResult.first();
            }
        }
        return retFamily;
    }

    protected QueryResult rank(MongoDBCollection collection, Bson query, String groupByField, String idField, int numResults, boolean asc) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new QueryResult();
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

            return collection.aggregate(Arrays.asList(match, project, group, sort, limit), new QueryOptions());
        }
    }

    protected QueryResult rank(MongoDBCollection collection, Bson query, List<String> groupByField, String idField, int numResults,
                               boolean asc) {

        if (groupByField == null || groupByField.isEmpty()) {
            return new QueryResult();
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

            return collection.aggregate(Arrays.asList(match, project, group, sort, limit), new QueryOptions());
        }
    }

    protected QueryResult groupBy(MongoDBCollection collection, Bson query, String groupByField, String idField, QueryOptions options) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new QueryResult();
        }

        if (groupByField.contains(",")) {
            // call to multiple groupBy if commas are present
            return groupBy(collection, query, Arrays.asList(groupByField.split(",")), idField, options);
        } else {
            return groupBy(collection, query, Arrays.asList(groupByField), idField, options);
        }
    }

    protected QueryResult groupBy(MongoDBCollection collection, Bson query, List<String> groupByField, String idField,
                                  QueryOptions options) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new QueryResult();
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
            id.append(s, "$" + s);
        }
        Bson group;
        if (options.getBoolean(QueryOptions.COUNT, false)) {
            group = Aggregates.group(id, Accumulators.sum(QueryOptions.COUNT, 1));
        } else {
            group = Aggregates.group(id, Accumulators.addToSet("items", "$" + idField));
        }
        return collection.aggregate(Arrays.asList(match, project, group), options);
//        }
    }

    /**
     * Create a date projection if included in the includeGroupByFields, removes the date fields from includeGroupByFields and
     * add them to groupByFields if not there.
     * Only for groupBy methods.
     *
     * @param includeGroupByFields List containing the fields to be included in the projection.
     * @param groupByFields List containing the fields by which the group by will be done.
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

    protected void unmarkPermissionRule(MongoDBCollection collection, long studyId, String permissionRuleId) {
        Bson query = new Document()
                .append(PRIVATE_STUDY_ID, studyId)
                .append(PERMISSION_RULES_APPLIED, permissionRuleId);
        Bson update = Updates.pull(PERMISSION_RULES_APPLIED, permissionRuleId);

        collection.update(query, update, new QueryOptions("multi", true));
    }

}
