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

import com.fasterxml.jackson.databind.*;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.AbstractCatalogDBAdaptor;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptor.PRIVATE_ID;

/**
 * Created by imedina on 21/11/14.
 */
class CatalogMongoDBUtils {

    public static final Set<String> DATASTORE_OPTIONS = Arrays.asList("include", "exclude", "sort", "limit", "skip").stream()
            .collect(Collectors.toSet());
    public static final Set<String> OTHER_OPTIONS = Arrays.asList("of", "sid", "sessionId", "metadata", "includeProjects",
            "includeStudies", "includeFiles", "includeJobs", "includeSamples").stream().collect(Collectors.toSet());
    //    public static final Pattern OPERATION_PATTERN = Pattern.compile("^([^=<>~!]*)(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");
    public static final Pattern OPERATION_PATTERN = Pattern.compile("^()(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");
    static final String TO_REPLACE_DOTS = "&#46;";
    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;
    private static Map<Class, ObjectReader> jsonReaderMap;

    static {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        jsonObjectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonReaderMap = new HashMap<>();
    }

    @Deprecated
    static int getNewAutoIncrementId(MongoDBCollection metaCollection) {
        return getNewAutoIncrementId("idCounter", metaCollection);
    }

    @Deprecated
    static int getNewAutoIncrementId(String field, MongoDBCollection metaCollection) {
//        QueryResult<BasicDBObject> result = metaCollection.findAndModify(
//                new BasicDBObject("_id", CatalogMongoDBAdaptor.METADATA_OBJECT_ID),  //Query
//                new BasicDBObject(field, true),  //Fields
//                null,
//                new BasicDBObject("$inc", new BasicDBObject(field, 1)), //Update
//                new QueryOptions("returnNew", true),
//                BasicDBObject.class
//        );

        Bson query = Filters.eq("_id", CatalogMongoDBAdaptorFactory.METADATA_OBJECT_ID);
        Document projection = new Document(field, true);
        Bson inc = Updates.inc(field, 1);
        QueryOptions queryOptions = new QueryOptions("returnNew", true);
        QueryResult<Document> result = metaCollection.findAndUpdate(query, projection, null, inc, queryOptions);
//        return (int) Float.parseFloat(result.getResult().get(0).get(field).toString());
        return result.getResult().get(0).getInteger(field);
    }


    @Deprecated
    static void checkUserExist(String userId, MongoDBCollection userCollection) throws CatalogDBException {
        if (userId == null) {
            throw new CatalogDBException("userId param is null");
        }
        if (userId.equals("")) {
            throw new CatalogDBException("userId is empty");
        }
        if (userCollection.count(new Document(PRIVATE_ID, userId)).first().equals(Long.valueOf(0))) {
            throw CatalogDBException.idNotFound("User", userId);
        }

    }


    /*
    * Helper methods
    ********************/

    /**
     * Checks if the field {@link AclEntry#userId} is valid.
     *
     * The "userId" can be:
     *  - '*' referring to all the users. See {@link AclEntry#USER_OTHERS_ID}
     *  - '@{groupId}' referring to a {@link Group}. See {@link AclEntry#USER_OTHERS_ID}
     *  - '{userId}' referring to a specific user.
     *
     * @param dbAdaptorFactory dbAdaptorFactory
     * @param userId userId
     * @param studyId studyId
     * @throws CatalogDBException CatalogDBException
     */
    public static void checkAclUserId(CatalogDBAdaptorFactory dbAdaptorFactory, String userId, int studyId) throws CatalogDBException {
        if (userId.equals(AclEntry.USER_OTHERS_ID)) {
            return;
        } else if (userId.startsWith("@")) {
            String groupId = userId.substring(1);
            QueryResult<Group> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, null, groupId, null);
            if (queryResult.getNumResults() == 0) {
                throw CatalogDBException.idNotFound("Group", groupId);
            }
        } else {
            dbAdaptorFactory.getCatalogUserDBAdaptor().checkUserExists(userId);
        }
    }

    static User parseUser(QueryResult<Document> result) throws CatalogDBException {
        return parseObject(result, User.class);
    }

    static List<Study> parseStudies(QueryResult<Document> result) throws CatalogDBException {
        return parseObjects(result, Study.class);
    }

    static File parseFile(QueryResult<Document> result) throws CatalogDBException {
        return parseObject(result, File.class);
    }

    static List<File> parseFiles(QueryResult<Document> result) throws CatalogDBException {
        return parseObjects(result, File.class);
    }

    static Job parseJob(QueryResult<Document> result) throws CatalogDBException {
        return parseObject(result, Job.class);
    }

    static List<Job> parseJobs(QueryResult<Document> result) throws CatalogDBException {
        return parseObjects(result, Job.class);
    }

    static List<Sample> parseSamples(QueryResult<Document> result) throws CatalogDBException {
        return parseObjects(result, Sample.class);
    }

    static Sample parseSample(QueryResult<Document> result) throws CatalogDBException {
        return parseObject(result, Sample.class);
    }

    static <T> List<T> parseObjects(QueryResult<Document> result, Class<T> tClass) throws CatalogDBException {
        LinkedList<T> objects = new LinkedList<>();
        ObjectReader objectReader = getObjectReader(tClass);
        try {
            for (Document document : result.getResult()) {
                document.remove("_id");
                document.remove("_projectId");
//                objects.add(objectReader.<T>readValue(restoreDotsInKeys(document).toString()));
                objects.add(objectReader.<T>readValue(restoreDotsInKeys(document.toJson())));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
        return objects;
    }

    static <T> T parseObject(QueryResult<Document> result, Class<T> tClass) throws CatalogDBException {
        if (result.getResult().isEmpty()) {
            return null;
        }
        try {
            result.first().remove("_id");
            result.first().remove("_studyId");
//            String s = result.first().toJson();
            String s = jsonObjectWriter.writeValueAsString(result.first());
//            return getObjectReader(tClass).readValue(restoreDotsInKeys(result.first().toJson()));
            return getObjectReader(tClass).readValue(restoreDotsInKeys(s));
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
    }

    static <T> T parseObject(Document result, Class<T> tClass) throws CatalogDBException {
        try {
            return getObjectReader(tClass).readValue(restoreDotsInKeys(result).toJson());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
    }

    @Deprecated
    static <T> T parseObject(DBObject result, Class<T> tClass) throws CatalogDBException {
        try {
            return getObjectReader(tClass).readValue(restoreDotsInKeys(result).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
    }

    public static <T> ObjectReader getObjectReader(Class<T> tClass) {
        if (!jsonReaderMap.containsKey(tClass)) {
            jsonReaderMap.put(tClass, jsonObjectMapper.reader(tClass));
        }
        return jsonReaderMap.get(tClass);
    }

    @Deprecated
    static DBObject getDbObject(Object object, String objectName) throws CatalogDBException {
        DBObject dbObject;
        String jsonString = null;
        try {
            jsonString = jsonObjectWriter.writeValueAsString(object);
            dbObject = (DBObject) JSON.parse(jsonString);
            dbObject = replaceDotsInKeys(dbObject);
        } catch (Exception e) {
            throw new CatalogDBException("Error while writing to Json : " + objectName + (jsonString == null ? "" : (" -> " + jsonString)
            ), e);
        }
        return dbObject;
    }

    static Document getMongoDBDocument(Object object, String objectName) throws CatalogDBException {
        Document document;
        String jsonString = null;
        try {
            jsonString = jsonObjectWriter.writeValueAsString(object);
            document = Document.parse(jsonString);
            document = replaceDotsInKeys(document);
        } catch (Exception e) {
            throw new CatalogDBException("Error while writing to Json : " + objectName + (jsonString == null
                    ? ""
                    : (" -> " + jsonString)), e);
        }
        return document;
    }
//    static final String TO_REPLACE_DOTS = "\uff0e";

    /***
     * Scan all the DBObject and replace all the dots in keys with.
     * @param object object
     * @param <T> T
     * @return T
     */
    static <T> T replaceDotsInKeys(T object) {
        return replaceInKeys(object, ".", TO_REPLACE_DOTS);
    }

    static <T> T restoreDotsInKeys(T object) {
        return replaceInKeys(object, TO_REPLACE_DOTS, ".");
    }

    static <T> T replaceInKeys(T object, String target, String replacement) {
        if (object instanceof Document) {
            Document document = (Document) object;
            List<String> keys = new ArrayList<>();
            for (String s : document.keySet()) {
                if (s.contains(target)) {
                    keys.add(s);
                }
                replaceInKeys(document.get(s), target, replacement);
            }
            for (String key : keys) {
                Object value = document.remove(key);
                key = key.replace(target, replacement);
                document.put(key, value);
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
     * <p>
     * Include and Exclude options are as absolute routes. This method removes all the values that are not in the
     * specified route. For the values in the route, the route is removed.
     * <p>
     * [
     * name,
     * projects.id,
     * projects.studies.id,
     * projects.studies.alias,
     * projects.studies.name
     * ]
     * <p>
     * with route = "projects.studies.", then
     * <p>
     * [
     * id,
     * alias,
     * name
     * ]
     *
     * @param options options
     * @param route route
     * @return QueryOptions
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
            if (list != null && !list.isEmpty()) {
                for (String s : list) {
                    if (s.startsWith(route)) {
                        filteredList.add(s.substring(length));
                    }
                }
                if (listName.equals("include")) {
                    filteredList.add("id");
                    filteredList.add(PRIVATE_ID);
                } else if (listName.equals("exclude")) {
                    filteredList.remove("id");
                    filteredList.remove(PRIVATE_ID);
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

    static void filterEnumParams(ObjectMap parameters, Map<String, Object> filteredParams, Map<String, Class<? extends Enum>>
            acceptedParams) throws CatalogDBException {
        for (Map.Entry<String, Class<? extends Enum>> e : acceptedParams.entrySet()) {
            if (parameters.containsKey(e.getKey())) {
                String parameterValue = parameters.getString(e.getKey());
                Set<String> set = (Set<String>) EnumSet.allOf(e.getValue()).stream().map(Object::toString).collect(Collectors.toSet());
                if (!set.contains(parameterValue)) {
                    throw new CatalogDBException("Invalid parameter { " + e.getKey() + ": \"" + parameterValue + "\" }. Accepted values "
                            + "from Enum " + e.getValue() + " " + EnumSet.allOf(e.getValue()));
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
        return DATASTORE_OPTIONS.contains(key);
    }

    static boolean isOtherKnownOption(String key) {
        return OTHER_OPTIONS.contains(key);
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


    public static void addAnnotationQueryFilter(String optionKey, QueryOptions options, List<DBObject> annotationSetFilter, Map<String,
            Variable> variableMap) throws CatalogDBException {
        // Annotation Filters
        final String sepAnd = ";";
        final String sepOr = ",";
        final String sepIs = ":";

        for (String annotation : options.getAsStringList(optionKey, sepAnd)) {
            String[] split = annotation.split(sepIs, 2);
            if (split.length != 2) {
                throw new CatalogDBException("Malformed annotation query : " + annotation);
            }
            final String variableId;
            final String route;
            if (split[0].contains(".")) {
                String[] variableIdRoute = split[0].split("\\.", 2);
                variableId = variableIdRoute[0];
                route = "." + variableIdRoute[1];
            } else {
                variableId = split[0];
                route = "";
            }
            String[] values = split[1].split(sepOr);

            AbstractCatalogDBAdaptor.FilterOption.Type type = AbstractCatalogDBAdaptor.FilterOption.Type.TEXT;

            if (variableMap != null) {
                Variable variable = variableMap.get(variableId);
                Variable.VariableType variableType = variable.getType();
                if (variable.getType() == Variable.VariableType.OBJECT) {
                    String[] routes = route.split("\\.");
                    for (String r : routes) {
                        if (variable.getType() != Variable.VariableType.OBJECT) {
                            throw new CatalogDBException("Unable to query variable " + split[0]);
                        }
                        if (variable.getVariableSet() != null) {
                            Map<String, Variable> subVariableMap = variable.getVariableSet().stream()
                                    .collect(Collectors.toMap(Variable::getId, Function.<Variable>identity()));
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
                    type = AbstractCatalogDBAdaptor.FilterOption.Type.BOOLEAN;

                } else if (variableType == Variable.VariableType.NUMERIC) {
                    type = AbstractCatalogDBAdaptor.FilterOption.Type.NUMERICAL;
                }
            }
            List<DBObject> queryValues = addCompQueryFilter(type, Arrays.asList(values), "value" + route, new LinkedList<DBObject>());
            annotationSetFilter.add(
                    new BasicDBObject("annotations",
                            new BasicDBObject("$elemMatch",
                                    new BasicDBObject(queryValues.get(0).toMap()).append("id", variableId)
                            )
                    )
            );
        }
    }

    static List<Document> addCompQueryFilter(AbstractCatalogDBAdaptor.FilterOption option, String optionKey, String queryKey, ObjectMap
            options,
                                             List<Document> andQuery) throws CatalogDBException {
        List<String> optionsList = options.getAsStringList(optionKey);
        if (queryKey == null) {
            queryKey = "";
        }
        return addCompQueryFilter(option.getType(), queryKey, optionsList, andQuery);
    }

    private static List<Document> addCompQueryFilter(AbstractCatalogDBAdaptor.FilterOption.Type type, String queryKey,
                                                     List<String> optionsList, List<Document> andQuery) throws CatalogDBException {

        ArrayList<Document> or = new ArrayList<>(optionsList.size());
        for (String option : optionsList) {
            Matcher matcher = OPERATION_PATTERN.matcher(option);
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
                        or.add(addNumberOperationQueryFilter(key, operator, doubleValue, new Document()));
                    } catch (NumberFormatException e) {
                        throw new CatalogDBException(e);
                    }
                    break;
                case TEXT:
                    or.add(addStringOperationQueryFilter(key, operator, filter, new Document()));
                    break;
                case BOOLEAN:
                    or.add(addBooleanOperationQueryFilter(key, operator, Boolean.parseBoolean(filter), new Document()));
                    break;
                default:
                    break;
            }
        }
        if (or.isEmpty()) {
            return andQuery;
        } else if (or.size() == 1) {
            andQuery.add(or.get(0));
        } else {
            andQuery.add(new Document("$or", or));
        }

        return andQuery;
    }

    @Deprecated
    static List<DBObject> addCompQueryFilter(AbstractCatalogDBAdaptor.FilterOption option, String optionKey, ObjectMap options, String
            queryKey,
                                             List<DBObject> andQuery) throws CatalogDBException {
        List<String> optionsList = options.getAsStringList(optionKey);
        if (queryKey == null) {
            queryKey = "";
        }
        return addCompQueryFilter(option.getType(), optionsList, queryKey, andQuery);
    }

    @Deprecated
    private static List<DBObject> addCompQueryFilter(AbstractCatalogDBAdaptor.FilterOption.Type type, List<String> optionsList,
                                                     String queryKey, List<DBObject> andQuery) throws CatalogDBException {

        ArrayList<DBObject> or = new ArrayList<>(optionsList.size());
        for (String option : optionsList) {
            Matcher matcher = OPERATION_PATTERN.matcher(option);
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
                default:
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

    @Deprecated
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

    static Document addStringOperationQueryFilter(String queryKey, String op, String filter, Document query) throws CatalogDBException {
        switch (op) {
            case "<":
                query.put(queryKey, new Document("$lt", filter));
                break;
            case "<=":
                query.put(queryKey, new Document("$lte", filter));
                break;
            case ">":
                query.put(queryKey, new Document("$gt", filter));
                break;
            case ">=":
                query.put(queryKey, new Document("$gte", filter));
                break;
            case "!=":
                query.put(queryKey, new Document("$ne", filter));
                break;
            case "":
            case "=":
            case "==":
                query.put(queryKey, filter);
                break;
            case "~":
            case "=~":
                query.put(queryKey, new Document("$regex", filter));
                break;
            default:
                throw new CatalogDBException("Unknown numerical query operation " + op);
        }
        return query;
    }

    @Deprecated
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

    static Document addNumberOperationQueryFilter(String queryKey, String op, Number filter, Document query) throws CatalogDBException {
        switch (op) {
            case "<":
                query.put(queryKey, new Document("$lt", filter));
                break;
            case "<=":
                query.put(queryKey, new Document("$lte", filter));
                break;
            case ">":
                query.put(queryKey, new Document("$gt", filter));
                break;
            case ">=":
                query.put(queryKey, new Document("$gte", filter));
                break;
            case "!=":
                query.put(queryKey, new Document("$ne", filter));
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

    @Deprecated
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

    static Document addBooleanOperationQueryFilter(String queryKey, String op, Boolean filter, Document query) throws CatalogDBException {
        switch (op) {
            case "!=":
                query.put(queryKey, new Document("$ne", filter));
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
                default:
                    break;
            }
        } catch (NumberFormatException e) {
            throw new CatalogDBException(e);
        }
        return query;
    }


}
