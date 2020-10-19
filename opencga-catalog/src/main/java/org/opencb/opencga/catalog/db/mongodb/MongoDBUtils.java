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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.AbstractDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.InterpretationManager;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.user.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.PRIVATE_ID;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.PRIVATE_UID;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by imedina on 21/11/14.
 */
class MongoDBUtils {

    // Special queryOptions keys
    public static final Set<String> DATASTORE_OPTIONS = Arrays.asList("include", "exclude", "sort", "limit", "skip").stream()
            .collect(Collectors.toSet());
    public static final Set<String> OTHER_OPTIONS = Arrays.asList("of", "sid", "sessionId", "metadata", "includeProjects",
            "includeStudies", "includeFiles", "includeJobs", "includeSamples").stream().collect(Collectors.toSet());
    //    public static final Pattern OPERATION_PATTERN = Pattern.compile("^([^=<>~!]*)(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");
    public static final Pattern OPERATION_PATTERN = Pattern.compile("^()(<=?|>=?|!==?|!?=?~|==?=?)([^=<>~!]+.*)$");
    @Deprecated
    public static final Pattern ANNOTATION_PATTERN = Pattern.compile("^([^=^<>~!$]+)([=^<>~!$]+.*)$");
    static final String TO_REPLACE_DOTS = "&#46;";
    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;
    private static Map<Class, ObjectReader> jsonReaderMap;

    protected static Logger logger = LoggerFactory.getLogger(MongoDBUtils.class);

    static {
        jsonObjectMapper = getDefaultObjectMapper();
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonReaderMap = new HashMap<>();
    }

    @Deprecated
    static long getNewAutoIncrementId(String field, MongoDBCollection metaCollection) {

        Bson query = Filters.eq(PRIVATE_ID, MongoDBAdaptorFactory.METADATA_OBJECT_ID);
        Document projection = new Document(field, true);
        Bson inc = Updates.inc(field, 1);
        QueryOptions queryOptions = new QueryOptions("returnNew", true);
        DataResult<Document> result = metaCollection.findAndUpdate(query, projection, null, inc, queryOptions);
//        return (int) Float.parseFloat(result.getResults().get(0).get(field).toString());
        return result.getResults().get(0).getInteger(field);
    }

    /*
     * Helper methods
     ********************/

    static User parseUser(DataResult<Document> result) throws CatalogDBException {
        return parseObject(result, User.class);
    }

    static List<Study> parseStudies(DataResult<Document> result) throws CatalogDBException {
        return parseObjects(result, Study.class);
    }

    static <T> List<T> parseObjects(DataResult<Document> result, Class<T> tClass) throws CatalogDBException {
        LinkedList<T> objects = new LinkedList<>();
        ObjectReader objectReader = getObjectReader(tClass);
        try {
            for (Document document : result.getResults()) {
//                document.remove("_id");
//                document.remove("_projectId");
                objects.add(objectReader.<T>readValue(restoreDotsInKeys(jsonObjectWriter.writeValueAsString(document))));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
        return objects;
    }

    static <T> T parseObject(DataResult<Document> result, Class<T> tClass) throws CatalogDBException {
        if (result.getResults().isEmpty()) {
            return null;
        }
        try {
            String s = jsonObjectWriter.writeValueAsString(result.first());
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
     * specified route. For the values in the route, the route is removed. Also, if there are additional options such as INCLUDE_ACLS,
     * it will add the proper field to the include filter.
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
                    } else {
                        filteredList.add(s);
                    }
                }
                if (listName.equals("include")) {
                    filteredList.add(PRIVATE_UID);
                    if (options.getBoolean(DBAdaptor.INCLUDE_ACLS)) {
                        filteredList.add(AuthorizationMongoDBAdaptor.QueryParams.ACL.key());
                    }
                } else if (listName.equals("exclude")) {
                    filteredList.remove(PRIVATE_UID);
                }
                filteredOptions.put(listName, filteredList);
            }
        }
        return filteredOptions;
    }

    static void filterBooleanParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedParams) {
        for (String s : acceptedParams) {
            if (parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getBoolean(s));
            }
        }
    }

    static void filterStringParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedParams) {
        for (String s : acceptedParams) {
            if (parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getString(s));
            }
        }
    }

    static void filterStringListParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedParams) {
        for (String s : acceptedParams) {
            if (parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getAsStringList(s));
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

    static void filterLongListParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedLongListParams) {
        for (String s : acceptedLongListParams) {
            if (parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getAsLongList(s));
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
                    Document document = getMongoDBDocument(map, s);
                    if (map.size() > 0) {
                        for (Map.Entry<String, Object> entry : map.entrySet()) {
                            filteredParams.put(s + "." + entry.getKey(), document.get(entry.getKey()));
                        }
                    } else {
                        filteredParams.put(s, document);
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
                Document document;
                try {
                    if (parameters.get(s) instanceof List<?>) {
                        List<Object> originalList = parameters.getAsList(s);
                        List<Document> documentList = new ArrayList<>(originalList.size());
                        for (Object object : originalList) {
                            documentList.add(getMongoDBDocument(object, s));
                        }
                        filteredParams.put(s, documentList);
                    } else {
                        document = getMongoDBDocument(parameters.get(s), s);
                        filteredParams.put(s, document);
                    }
                } catch (CatalogDBException e) {
                    logger.warn("Skipping key '" + s + "': " + e.getMessage(), e);
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

    protected static void nestedPut(String key, Object value, Map<String, Object> document) {
        if (key.contains(".")) {
            String[] keys = key.split("\\.");
            Map<String, Object> auxDocument = document;
            for (int i = 0; i < keys.length; i++) {
                String tmpKey = keys[i];
                if (i + 1 == keys.length) {
                    auxDocument.put(tmpKey, value);
                } else {
                    if (auxDocument.get(tmpKey) == null) {
                        auxDocument.put(tmpKey, new HashMap<>());
                    }
                    auxDocument = (Map<String, Object>) auxDocument.get(tmpKey);
                }
            }
        } else {
            document.put(key, value);
        }
    }

    static boolean isDataStoreOption(String key) {
        return DATASTORE_OPTIONS.contains(key);
    }

    static boolean isOtherKnownOption(String key) {
        return OTHER_OPTIONS.contains(key);
    }

    /**
     * Changes the format of the queries. Queries retrieved from the WS come as "annotation": "nestedKey.subkey=5,sex=male".
     * That will be changed to "annotation.nestedKey.subkey" : "=5"; "annotation.sex": "=male"
     *
     * @param queryKey queryKey
     * @param query queryObject
     */
    public static void fixComplexQueryParam(String queryKey, Query query) {
        if (!query.containsKey(queryKey)) {
            return;
        }

        List<String> valueList = query.getAsStringList(queryKey, ";");
        for (String annotation : valueList) {
            Matcher matcher = ANNOTATION_PATTERN.matcher(annotation);
            String key;
            String queryValueString;
            if (matcher.find()) {
                key = matcher.group(1);
                if (!key.startsWith(queryKey + ".")) {
                    key = queryKey + "." + key;
                }
                queryValueString = matcher.group(2);

                query.append(key, queryValueString);
            }
        }

        // Remove the current query
        query.remove(queryKey);
    }


    /**
     * Parse the query values from the ontology terms to bson format.
     * At this point, any value that we find in the query (as comma separated or as a list), will be looked for in the id and name
     * attributes of the ontologyTerm java bean.
     *
     * Example: ontologyTerms: X,Y
     *          This will be transformed to something like ontologyTerms.id == X || ontologyTerms.name == X || ontologyTerms.id == Y ||
     *          ontologyTerms.name == Y)
     *
     * @param mongoKey Key corresponding to the data model to know how it is stored in mongoDB. (If not altered, ontologyTerms).
     * @param queryKey Key by which the values will be retrieved from the query.
     * @param query Query object containing all the query keys and values to parse. Only to get the ones regarding ontology terms.
     * @param bsonList List to which we will add the ontology terms search.
     */
    public static void addOntologyQueryFilter(String mongoKey, String queryKey, Query query, List<Bson> bsonList) {
        List<String> ontologyValues = query.getAsStringList(queryKey);
        Bson ontologyId = MongoDBQueryUtils.createFilter(mongoKey + ".id", ontologyValues);
        Bson ontologyName = MongoDBQueryUtils.createFilter(mongoKey + ".name", ontologyValues);
        bsonList.add(Filters.or(ontologyId, ontologyName));
    }

    public static void addAnnotationQueryFilter(String optionKey, Query query, Map<String, Variable> variableMap,
                                                List<Bson> annotationSetFilter) throws CatalogDBException {
        // Annotation Filter
        final String sepOr = ",";

        String annotationKey;
        if (optionKey.startsWith("annotation.")) {
            annotationKey = optionKey.substring("annotation.".length());
        } else {
            throw new CatalogDBException("Wrong annotation query. Expects: {\"annotation.<variable>\" , <operator><value> } ");
        }
        String annotationValue = query.getString(optionKey);

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
                                .collect(Collectors.toMap(Variable::getId, Function.<Variable>identity()));
                        if (subVariableMap.containsKey(r)) {
                            variable = subVariableMap.get(r);
                            variableType = variable.getType();
                        }
                    } else {
                        variableType = Variable.VariableType.STRING;
                        break;
                    }
                }
            }
            if (variableType == Variable.VariableType.BOOLEAN) {
                type = QueryParam.Type.BOOLEAN;

            } else if (variableType == Variable.VariableType.DOUBLE) {
                type = QueryParam.Type.DECIMAL;
            } else if (variableType == Variable.VariableType.INTEGER) {
                type = QueryParam.Type.INTEGER;
            }
        }

        List<Document> valueList = addCompQueryFilter(type, "value" + route, Arrays.asList(values), new ArrayList<>());
        annotationSetFilter.add(
                Filters.elemMatch("annotations", Filters.and(
                        Filters.eq("name", variableId),
                        valueList.get(0)
                ))
        );
    }

    @Deprecated
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

            AbstractDBAdaptor.FilterOption.Type type = AbstractDBAdaptor.FilterOption.Type.TEXT;

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
                            variableType = Variable.VariableType.STRING;
                            break;
                        }
                    }
                }
                if (variableType == Variable.VariableType.BOOLEAN) {
                    type = AbstractDBAdaptor.FilterOption.Type.BOOLEAN;

                } else if (variableType == Variable.VariableType.DOUBLE) {
                    type = AbstractDBAdaptor.FilterOption.Type.NUMERICAL;
                }
            }
            List<DBObject> queryValues = addCompQueryFilter(type, Arrays.asList(values), "value" + route, new LinkedList<DBObject>());
            annotationSetFilter.add(
                    new BasicDBObject("annotations",
                            new BasicDBObject("$elemMatch",
                                    new BasicDBObject(queryValues.get(0).toMap()).append("name", variableId)
                            )
                    )
            );
        }
    }

    static List<Document> addCompQueryFilter(QueryParam option, String optionKey, String queryKey,
                                             ObjectMap options, List<Document> andQuery) throws CatalogDBException {
        List<String> optionsList = options.getAsStringList(optionKey);
        if (queryKey == null) {
            queryKey = "";
        }
        return addCompQueryFilter(option.type(), queryKey, optionsList, andQuery);
    }

    public static String getOperator(String queryValue) {
        Matcher matcher = OPERATION_PATTERN.matcher(queryValue);
        if (matcher.find()) {
            return matcher.group(2);
        }
        return null;
    }

    public static List<Document> addCompQueryFilter(QueryParam.Type type, String queryKey, List<String> optionsList,
                                                    List<Document> andQuery) throws CatalogDBException {

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
                key = queryKey;
                filter = matcher.group(3);
            }
            if (key.isEmpty()) {
                throw new CatalogDBException("Unknown filter operation: " + option + " . Missing key");
            }
            switch (type) {
                case INTEGER:
                case INTEGER_ARRAY:
                    try {
                        int intValue = Integer.parseInt(filter);
                        or.add(addNumberOperationQueryFilter(key, operator, intValue));
                    } catch (NumberFormatException e) {
                        throw new CatalogDBException("Expected an integer value - " + e.getMessage(), e);
                    }
                    break;
                case DECIMAL:
                case DOUBLE:
                case DECIMAL_ARRAY:
                    try {
                        double doubleValue = Double.parseDouble(filter);
                        or.add(addNumberOperationQueryFilter(key, operator, doubleValue));
                    } catch (NumberFormatException e) {
                        throw new CatalogDBException("Expected a double value - " + e.getMessage(), e);
                    }
                    break;
                case TEXT:
                case TEXT_ARRAY:
                    or.add(addStringOperationQueryFilter(key, operator, filter));
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
    static List<DBObject> addCompQueryFilter(AbstractDBAdaptor.FilterOption option, String optionKey, ObjectMap options, String
            queryKey,
                                             List<DBObject> andQuery) throws CatalogDBException {
        List<String> optionsList = options.getAsStringList(optionKey);
        if (queryKey == null) {
            queryKey = "";
        }
        return addCompQueryFilter(option.getType(), optionsList, queryKey, andQuery);
    }

    @Deprecated
    private static List<DBObject> addCompQueryFilter(AbstractDBAdaptor.FilterOption.Type type, List<String> optionsList,
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

    static Document addStringOperationQueryFilter(String queryKey, String op, String filter) throws CatalogDBException {
        Document query;
        switch (op) {
            case "<":
                query = new Document(queryKey, new Document("$lt", filter));
                break;
            case "<=":
                query = new Document(queryKey, new Document("$lte", filter));
                break;
            case ">":
                query = new Document(queryKey, new Document("$gt", filter));
                break;
            case ">=":
                query = new Document(queryKey, new Document("$gte", filter));
                break;
            case "!=":
                query = new Document(queryKey, new Document("$ne", filter));
                break;
            case "":
            case "=":
            case "==":
                query = new Document(queryKey, filter);
                break;
            case "~":
            case "=~":
                query = new Document(queryKey, new Document("$regex", filter));
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

    static Document addNumberOperationQueryFilter(String queryKey, String op, Number filter) throws CatalogDBException {
        Document query;
        switch (op) {
            case "<":
                query = new Document(queryKey, new Document("$lt", filter));
                break;
            case "<=":
                query = new Document(queryKey, new Document("$lte", filter));
                break;
            case ">":
                query = new Document(queryKey, new Document("$gt", filter));
                break;
            case ">=":
                query = new Document(queryKey, new Document("$gte", filter));
                break;
            case "!=":
                query = new Document(queryKey, new Document("$ne", filter));
                break;
            case "":
            case "=":
            case "==":
                query = new Document(queryKey, filter);
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


    static boolean isDuplicateKeyException(MongoWriteException e) {
        return ErrorCategory.fromErrorCode(e.getCode()) == ErrorCategory.DUPLICATE_KEY;
    }

    static CatalogDBException ifDuplicateKeyException(Supplier<? extends CatalogDBException> producer, MongoWriteException e) {
        if (isDuplicateKeyException(e)) {
            return producer.get();
        } else {
            throw e;
        }
    }
}
