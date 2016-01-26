/*
 * Copyright 2015 OpenCB
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

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.AbstractCatalogDBAdaptor;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jacobo on 12/09/14.
 */
public class CatalogMongoDBAdaptor extends AbstractCatalogDBAdaptor {

    static final String PRIVATE_ID = "_id";
    static final String PRIVATE_PROJECT_ID = "_projectId";
    static final String PRIVATE_STUDY_ID = "_studyId";
    static final String FILTER_ROUTE_STUDIES = "projects.studies.";
    static final String FILTER_ROUTE_INDIVIDUALS = "projects.studies.individuals.";
    static final String FILTER_ROUTE_SAMPLES = "projects.studies.samples.";
    static final String FILTER_ROUTE_FILES = "projects.studies.files.";
    static final String FILTER_ROUTE_JOBS = "projects.studies.jobs.";

    protected CatalogMongoDBAdaptorFactory dbAdaptorFactory;

    public CatalogMongoDBAdaptor(Logger logger) {
        super(logger);
    }

    protected int getNewId() {
//        return CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);
        return dbAdaptorFactory.getCatalogMetaDBAdaptor().getNewAutoIncrementId();
    }

    //    protected void addIntegerOrQuery(String mongoDbField, String queryParam, Query query, List<Bson> andBsonList) {
//        if (query != null && query.containsKey(queryParam)) {
//            addIntegerOrQuery(mongoDbField, query.getAsIntegerList(queryParam), andBsonList);
//        }
//    }
//
//    protected void addIntegerOrQuery(String mongoDbField, List<Integer> queryValues, List<Bson> andBsonList) {
//        if (queryValues.size() == 1) {
//            andBsonList.add(Filters.eq(mongoDbField, queryValues.get(0)));
//        } else {
//            List<Bson> orBsonList = new ArrayList<>(queryValues.size());
//            for (Integer queryItem : queryValues) {
//                orBsonList.add(Filters.eq(mongoDbField, queryItem));
//            }
//            andBsonList.add(Filters.or(orBsonList));
//        }
//    }

//    protected void addBooleanOrQuery(String mongoDbField, String queryParam, Query query, List<Bson> andBsonList) {
//        addQueryFilter(mongoDbField, queryParam, query, MongoDBQueryUtils.ParamType.BOOLEAN, MongoDBQueryUtils.ComparisonOperator.EQUAL,
//                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
//    }

    protected void addIntegerOrQuery(String mongoDbField, String queryParam, Query query, List<Bson> andBsonList) {
        addQueryFilter(mongoDbField, queryParam, query, MongoDBQueryUtils.ParamType.INTEGER, MongoDBQueryUtils.ComparisonOperator.EQUAL,
                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
    }

    protected void addStringOrQuery(String mongoDbField, String queryParam, Query query, List<Bson> andBsonList) {
        addQueryFilter(mongoDbField, queryParam, query, MongoDBQueryUtils.ParamType.STRING, MongoDBQueryUtils.ComparisonOperator.EQUAL,
                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
    }

    protected void addStringOrQuery(String mongoDbField, String queryParam, Query query, MongoDBQueryUtils.ComparisonOperator
            comparisonOperator, List<Bson> andBsonList) {
        addQueryFilter(mongoDbField, queryParam, query, MongoDBQueryUtils.ParamType.STRING, comparisonOperator,
                MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
    }

//    protected void addStringOrQuery(String mongoDbField, List<String> queryValues, List<Bson> andBsonList) {
//        if (queryValues.size() == 1) {
//            andBsonList.add(Filters.eq(mongoDbField, queryValues.get(0)));
//        } else {
//            List<Bson> orBsonList = new ArrayList<>(queryValues.size());
//            for (String queryItem : queryValues) {
//                orBsonList.add(Filters.eq(mongoDbField, queryItem));
//            }
//            andBsonList.add(Filters.or(orBsonList));
//        }
//    }

    protected void addQueryFilter(String mongoDbField, String queryParam, Query query, MongoDBQueryUtils.ParamType paramType,
                                  MongoDBQueryUtils.ComparisonOperator comparisonOperator, MongoDBQueryUtils.LogicalOperator operator,
                                  List<Bson> andBsonList) {
        if (query != null && query.getString(queryParam) != null) {
            Bson filter = MongoDBQueryUtils.createFilter(mongoDbField, queryParam, query, paramType, comparisonOperator, operator);
            if (filter != null) {
                andBsonList.add(filter);
            }
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
            Bson match = Aggregates.match(query);
            Bson project = Aggregates.project(Projections.include(groupByField, idField));
            Bson group;
            if (options.getBoolean("count", false)) {
                group = Aggregates.group("$" + groupByField, Accumulators.sum("count", 1));
            } else {
                group = Aggregates.group("$" + groupByField, Accumulators.addToSet("features", "$" + idField));
            }
            return collection.aggregate(Arrays.asList(match, project, group), options);
        }
    }

    protected QueryResult groupBy(MongoDBCollection collection, Bson query, List<String> groupByField, String idField, QueryOptions
            options) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new QueryResult();
        }

        if (groupByField.size() == 1) {
            // if only one field then we call to simple groupBy
            return groupBy(collection, query, groupByField.get(0), idField, options);
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
            Bson group;
            if (options.getBoolean("count", false)) {
                group = Aggregates.group(id, Accumulators.sum("count", 1));
            } else {
                group = Aggregates.group(id, Accumulators.addToSet("features", "$" + idField));
            }
            return collection.aggregate(Arrays.asList(match, project, group), options);
        }
    }

}
