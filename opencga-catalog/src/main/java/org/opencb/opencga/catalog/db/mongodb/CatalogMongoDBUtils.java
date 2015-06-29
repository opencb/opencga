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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by imedina on 21/11/14.
 */
class CatalogMongoDBUtils {

    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;
    private static Map<Class, ObjectReader> jsonReaderMap;

    public static final Set<String> datastoreOptions = Arrays.asList("include", "exclude", "sort", "limit", "skip").stream().collect(Collectors.toSet());
    public static final Set<String> otherOptions = Arrays.asList("of", "sid", "metadata", "includeProjects", "includeStudies", "includeFiles", "includeJobs", "includeSamples").stream().collect(Collectors.toSet());
    //    public static final Pattern operationPattern = Pattern.compile("^([^=<>~!]*)(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");
    public static final Pattern operationPattern = Pattern.compile("^()(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");

    static {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonReaderMap = new HashMap<>();
    }

    static int getNewAutoIncrementId(MongoDBCollection metaCollection) {
        return getNewAutoIncrementId("idCounter", metaCollection);
    }

    static int getNewAutoIncrementId(String field, MongoDBCollection metaCollection) {
        QueryResult<BasicDBObject> result = metaCollection.findAndModify(
                new BasicDBObject("_id", CatalogMongoDBAdaptor.METADATA_OBJECT_ID),  //Query
                new BasicDBObject(field, true),  //Fields
                null,
                new BasicDBObject("$inc", new BasicDBObject(field, 1)), //Update
                new QueryOptions("returnNew", true),
                BasicDBObject.class
        );
//        return (int) Float.parseFloat(result.getResult().get(0).get(field).toString());
        return result.getResult().get(0).getInt(field);
    }

    static void checkUserExist(String userId, boolean exists, MongoDBCollection UserMongoDBCollection) throws CatalogDBException {
        if (userId == null) {
            throw new CatalogDBException("userId param is null");
        }
        if (userId.equals("")) {
            throw new CatalogDBException("userId is empty");
        }

    }


    /*
    * Helper methods
    ********************/


    static User parseUser(QueryResult<DBObject> result) throws CatalogDBException {
        return parseObject(result, User.class);
    }

    static List<Study> parseStudies(QueryResult<DBObject> result) throws CatalogDBException {
        return parseObjects(result, Study.class);
    }

    static File parseFile(QueryResult<DBObject> result) throws CatalogDBException {
        return parseObject(result, File.class);
    }

    static List<File> parseFiles(QueryResult<DBObject> result) throws CatalogDBException {
        return parseObjects(result, File.class);
    }

    static Job parseJob(QueryResult<DBObject> result) throws CatalogDBException {
        return parseObject(result, Job.class);
    }

    static List<Job> parseJobs(QueryResult<DBObject> result) throws CatalogDBException {
        return parseObjects(result, Job.class);
    }

    static List<Sample> parseSamples(QueryResult<DBObject> result) throws CatalogDBException {
        return parseObjects(result, Sample.class);
    }

    static <T> List<T> parseObjects(QueryResult<DBObject> result, Class<T> tClass) throws CatalogDBException {
        LinkedList<T> objects = new LinkedList<>();
        ObjectReader objectReader = getObjectReader(tClass);
        try {
            for (DBObject object : result.getResult()) {
                objects.add(objectReader.<T>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
        return objects;
    }

    static <T> T parseObject(QueryResult<DBObject> result, Class<T> tClass) throws CatalogDBException {
        if (result.getResult().isEmpty()) {
            return null;
        }
        try {
            return getObjectReader(tClass).readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
    }


    static <T> T parseObject(DBObject result, Class<T> tClass) throws CatalogDBException {
        try {
            return getObjectReader(tClass).readValue(restoreDotsInKeys(result).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
    }

    private static <T> ObjectReader getObjectReader(Class<T> tClass) {
        if (!jsonReaderMap.containsKey(tClass)) {
            jsonReaderMap.put(tClass, jsonObjectMapper.reader(tClass));
        }
        return jsonReaderMap.get(tClass);
    }

    static DBObject getDbObject(Object object, String objectName) throws CatalogDBException {
        DBObject dbObject;
        try {
            dbObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(object));
            dbObject = replaceDotsInKeys(dbObject);
        } catch (Exception e) {
            throw new CatalogDBException("Error while writing to Json : " + objectName, e);
        }
        return dbObject;
    }

    static final String TO_REPLACE_DOTS = "&#46;";
//    static final String TO_REPLACE_DOTS = "\uff0e";

    /**
     * Scan all the DBObject and replace all the dots in keys with
     *
     * @param object
     * @return
     */


    static <T> T replaceDotsInKeys(T object) {
        return replaceInKeys(object, ".", TO_REPLACE_DOTS);
    }

    static <T> T restoreDotsInKeys(T object) {
        return replaceInKeys(object, TO_REPLACE_DOTS, ".");
    }

    static <T> T replaceInKeys(T object, String target, String replacement) {
        if (object instanceof DBObject) {
            DBObject dbObject = (DBObject) object;
            List<String> keys = new ArrayList<>();
            for (String s : dbObject.keySet()) {
                if (s.contains(target)) {
                    keys.add(s);
                }
                replaceInKeys(dbObject.get(s), target, replacement);
            }
            for (String key : keys) {
                Object value = dbObject.removeField(key);
                key = key.replace(target, replacement);
                dbObject.put(key, value);
            }
        } else if (object instanceof List) {
            for (Object o : ((List) object)) {
                replaceInKeys(o, target, replacement);
            }
        }
        return object;
    }

    /*  */


    /**
     * Filter "include" and "exclude" options.
     * <p/>
     * Include and Exclude options are as absolute routes. This method removes all the values that are not in the
     * specified route. For the values in the route, the route is removed.
     * <p/>
     * [
     * name,
     * projects.id,
     * projects.studies.id,
     * projects.studies.alias,
     * projects.studies.name
     * ]
     * <p/>
     * with route = "projects.studies.", then
     * <p/>
     * [
     * id,
     * alias,
     * name
     * ]
     *
     * @param options
     * @param route
     * @return
     */
    static QueryOptions filterOptions(QueryOptions options, String route) {
        if (options == null) {
            return null;
        }

        QueryOptions filteredOptions = new QueryOptions(options); //copy queryOptions

        String[] filteringLists = {"include", "exclude"};
        for (String listName : filteringLists) {
            List<String> list = filteredOptions.getAsStringList(listName);
            List<String> filteredList = new LinkedList<>();
            int length = route.length();
            if (list != null) {
                for (String s : list) {
                    if (s.startsWith(route)) {
                        filteredList.add(s.substring(length));
                    }
                }
                filteredOptions.put(listName, filteredList);
            }
        }
        return filteredOptions;
    }

    static void filterStringParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedParams) {
        for (String s : acceptedParams) {
            if (parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getString(s));
            }
        }
    }

    static void filterEnumParams(ObjectMap parameters, Map<String, Object> filteredParams, Map<String, Class<? extends Enum>> acceptedParams) throws CatalogDBException {
        for (Map.Entry<String, Class<? extends Enum>> e : acceptedParams.entrySet()) {
            if (parameters.containsKey(e.getKey())) {
                String parameterValue = parameters.getString(e.getKey());
                Set<String> set = (Set<String>) EnumSet.allOf(e.getValue()).stream().map(Object::toString).collect(Collectors.toSet());
                if (!set.contains(parameterValue)) {
                    throw new CatalogDBException("Invalid parameter { " + e.getKey() + ": \"" + parameterValue + "\" }. Accepted values from Enum " + e.getValue() + " " + EnumSet.allOf(e.getValue()));
                }
                filteredParams.put(e.getKey(), parameterValue);
            }
        }
    }

    static void filterIntegerListParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedIntegerListParams) {
        for (String s : acceptedIntegerListParams) {
            if (parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getAsIntegerList(s));
            }
        }
    }

    static void filterMapParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedMapParams) {
        for (String s : acceptedMapParams) {
            if (parameters.containsKey(s)) {
                ObjectMap map;
                if (parameters.get(s) instanceof Map) {
                    map = new ObjectMap(parameters.getMap(s));
                } else {
                    map = new ObjectMap(parameters.getString(s));
                }
                try {
                    DBObject dbObject = getDbObject(map, s);
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        filteredParams.put(s + "." + entry.getKey(), dbObject.get(entry.getKey()));
                    }
                } catch (CatalogDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static void filterObjectParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedMapParams) {
        for (String s : acceptedMapParams) {
            if (parameters.containsKey(s)) {
                DBObject dbObject = null;
                try {
                    dbObject = getDbObject(parameters.get(s), s);
                    filteredParams.put(s, dbObject);
                } catch (CatalogDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static void filterIntParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedIntParams) {
        for (String s : acceptedIntParams) {
            if (parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if (anInt != Integer.MIN_VALUE) {
                    filteredParams.put(s, anInt);
                }
            }
        }
    }

    static void filterLongParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedLongParams) {
        for (String s : acceptedLongParams) {
            if (parameters.containsKey(s)) {
                long aLong = parameters.getLong(s, Long.MIN_VALUE);
                if (aLong != Long.MIN_VALUE) {
                    filteredParams.put(s, aLong);
                }
            }
        }
    }

    /*  */

    static boolean isDataStoreOption(String key) {
        return datastoreOptions.contains(key);
    }

    static boolean isOtherKnownOption(String key) {
        return otherOptions.contains(key);
    }


    static void addQueryStringListFilter(String key, QueryOptions options, DBObject query) {
        addQueryStringListFilter(key, options, key, query);
    }

    static void addQueryStringListFilter(String optionKey, QueryOptions options, String queryKey, DBObject query) {
        if (options.containsKey(optionKey)) {
            List<String> stringList = options.getAsStringList(optionKey);
            if (stringList.size() > 1) {
                query.put(queryKey, new BasicDBObject("$in", stringList));
            } else if (stringList.size() == 1) {
                query.put(queryKey, stringList.get(0));
            }
        }
    }

    static void addQueryIntegerListFilter(String key, QueryOptions options, DBObject query) {
        addQueryIntegerListFilter(key, options, key, query);
    }

    static void addQueryIntegerListFilter(String optionKey, QueryOptions options, String queryKey, DBObject query) {
        if (options.containsKey(optionKey)) {
            List<Integer> integerList = options.getAsIntegerList(optionKey);
            if (integerList.size() > 1) {
                query.put(queryKey, new BasicDBObject("$in", integerList));
            } else if (integerList.size() == 1) {
                query.put(queryKey, integerList.get(0));
            }
        }
    }


    public static void addAnnotationQueryFilter(String optionKey, QueryOptions options, List<DBObject> annotationSetFilter, Map<String, Variable> variableMap) throws CatalogDBException {
        // Annotation Filters
        final String AND = ";";
        final String OR = ",";
        final String IS = ":";

        for (String annotation : options.getAsStringList(optionKey, AND)) {
            String[] split = annotation.split(IS, 2);
            if (split.length != 2) {
                throw new CatalogDBException("Malformed annotation query : " + annotation);
            }
            final String variableId;
            final String route;
            if (split[0].contains(".")) {
                String[] variableId_route = split[0].split("\\.", 2);
                variableId = variableId_route[0];
                route = "." + variableId_route[1];
            } else {
                variableId = split[0];
                route = "";
            }
            String[] values = split[1].split(OR);

            CatalogDBAdaptor.FilterOption.Type type = CatalogDBAdaptor.FilterOption.Type.TEXT;

            if (variableMap != null) {
                Variable variable = variableMap.get(variableId);
                Variable.VariableType variableType = variable.getType();
                if ( variable.getType() == Variable.VariableType.OBJECT) {
                    String[] routes = route.split("\\.");
                    for (String r : routes) {
                        if (variable.getType() != Variable.VariableType.OBJECT) {
                            throw new CatalogDBException("Unable to query variable " + split[0]);
                        }
                        if (variable.getVariableSet() != null) {
                            Map<String, Variable> subVariableMap = variable.getVariableSet().stream().collect(Collectors.toMap(Variable::getId, Function.<Variable>identity()));
                            if (subVariableMap.containsKey(r)) {
                                variable = subVariableMap.get(r);
                                variableType = variable.getType();
                            }
                        } else {
                            variableType = Variable.VariableType.TEXT;
                            break;
                        }
                    }
                }
                if (variableType == Variable.VariableType.BOOLEAN) {
                    type = CatalogDBAdaptor.FilterOption.Type.BOOLEAN;

                } else if (variableType == Variable.VariableType.NUMERIC) {
                    type = CatalogDBAdaptor.FilterOption.Type.NUMERICAL;
                }
            }
            List<DBObject> queryValues = addCompQueryFilter(type, Arrays.asList(values), "value" + route, new LinkedList<>());
            annotationSetFilter.add(
                    new BasicDBObject("annotations",
                            new BasicDBObject("$elemMatch",
                                    new BasicDBObject(queryValues.get(0).toMap()).append("id", variableId)
                            )
                    )
            );
        }
    }


    static List<DBObject> addCompQueryFilter(CatalogDBAdaptor.FilterOption option, String optionKey, QueryOptions options, String queryKey, List<DBObject> andQuery) throws CatalogDBException {
        List<String> optionsList = options.getAsStringList(optionKey);
        if (queryKey == null) {
            queryKey = "";
        }
        return addCompQueryFilter(option.getType(), optionsList, queryKey, andQuery);
    }

    static private List<DBObject> addCompQueryFilter(CatalogDBAdaptor.FilterOption.Type type, List<String> optionsList, String queryKey, List<DBObject> andQuery) throws CatalogDBException {

        ArrayList<DBObject> or = new ArrayList<>(optionsList.size());
        for (String option : optionsList) {
            Matcher matcher = operationPattern.matcher(option);
            String operator;
            String key;
            String filter;
            if (!matcher.find()) {
                operator = "";
                key = queryKey;
                filter = option;
            } else {
                operator = matcher.group(2);
//                if (queryKey.isEmpty()) {
//                    key = matcher.group(1);
//                } else {
//                    String separatorDot = matcher.group(1).isEmpty() ? "" : ".";
//                    key = queryKey + separatorDot + matcher.group(1);
//                }
                key = queryKey;
                filter = matcher.group(3);
            }
            if (key.isEmpty()) {
                throw new CatalogDBException("Unknown filter operation: " + option + " . Missing key");
            }
            switch (type) {
                case NUMERICAL:
                    try {
                        double doubleValue = Double.parseDouble(filter);
                        or.add(addNumberOperationQueryFilter(key, operator, doubleValue, new BasicDBObject()));
                    } catch (NumberFormatException e) {
                        throw new CatalogDBException(e);
                    }
                    break;
                case TEXT:
                    or.add(addStringOperationQueryFilter(key, operator, filter, new BasicDBObject()));
                    break;
                case BOOLEAN:
                    or.add(addBooleanOperationQueryFilter(key, operator, Boolean.parseBoolean(filter), new BasicDBObject()));
                    break;
            }
        }
        if (or.isEmpty()) {
            return andQuery;
        } else if (or.size() == 1) {
            andQuery.add(or.get(0));
        } else {
            andQuery.add(new BasicDBObject("$or", or));
        }

        return andQuery;
    }

    static DBObject addStringOperationQueryFilter(String queryKey, String op, String filter, DBObject query) throws CatalogDBException {
        switch (op) {
            case "<":
                query.put(queryKey, new BasicDBObject("$lt", filter));
                break;
            case "<=":
                query.put(queryKey, new BasicDBObject("$lte", filter));
                break;
            case ">":
                query.put(queryKey, new BasicDBObject("$gt", filter));
                break;
            case ">=":
                query.put(queryKey, new BasicDBObject("$gte", filter));
                break;
            case "!=":
                query.put(queryKey, new BasicDBObject("$ne", filter));
                break;
            case "":
            case "=":
            case "==":
                query.put(queryKey, filter);
                break;
            case "~":
            case "=~":
                query.put(queryKey, new BasicDBObject("$regex", filter));
                break;
            default:
                throw new CatalogDBException("Unknown numerical query operation " + op);
        }
        return query;
    }

    static DBObject addNumberOperationQueryFilter(String queryKey, String op, Number filter, DBObject query) throws CatalogDBException {
        switch (op) {
            case "<":
                query.put(queryKey, new BasicDBObject("$lt", filter));
                break;
            case "<=":
                query.put(queryKey, new BasicDBObject("$lte", filter));
                break;
            case ">":
                query.put(queryKey, new BasicDBObject("$gt", filter));
                break;
            case ">=":
                query.put(queryKey, new BasicDBObject("$gte", filter));
                break;
            case "!=":
                query.put(queryKey, new BasicDBObject("$ne", filter));
                break;
            case "":
            case "=":
            case "==":
                query.put(queryKey, filter);
                break;
            default:
                throw new CatalogDBException("Unknown string query operation " + op);
        }
        return query;
    }

    static DBObject addBooleanOperationQueryFilter(String queryKey, String op, Boolean filter, DBObject query) throws CatalogDBException {
        switch (op) {
            case "!=":
                query.put(queryKey, new BasicDBObject("$ne", filter));
                break;
            case "":
            case "=":
            case "==":
                query.put(queryKey, filter);
                break;
            default:
                throw new CatalogDBException("Unknown boolean query operation " + op);
        }
        return query;
    }

    @Deprecated
    static DBObject addCompQueryFilter(String queryKey, String op, String filter, DBObject query) throws CatalogDBException {
        try {
            switch (op) {
                case "<":
                    query.put(queryKey, new BasicDBObject("$lt", Double.parseDouble(filter)));
                    break;
                case "<=":
                    query.put(queryKey, new BasicDBObject("$lte", Double.parseDouble(filter)));
                    break;
                case ">":
                    query.put(queryKey, new BasicDBObject("$gt", Double.parseDouble(filter)));
                    break;
                case ">=":
                    query.put(queryKey, new BasicDBObject("$gte", Double.parseDouble(filter)));
                    break;
                case "==":
                    query.put(queryKey, new BasicDBObject("$eq", Double.parseDouble(filter)));
                    break;
                case "!=":
                    query.put(queryKey, new BasicDBObject("$ne", Double.parseDouble(filter)));
                    break;
                case "!~":
                case "!=~":
                    query.put(queryKey, new BasicDBObject("$not", new BasicDBObject("$regex", filter)));
                    break;
                case "~":
                case "=~":
                    query.put(queryKey, new BasicDBObject("$regex", filter));
                    break;
            }
        } catch (NumberFormatException e) {
            throw new CatalogDBException(e);
        }
        return query;
    }



}
