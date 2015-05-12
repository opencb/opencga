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
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 21/11/14.
 */
class CatalogMongoDBUtils {

    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;
    private static ObjectReader jsonFileReader;
    private static ObjectReader jsonUserReader;
    private static ObjectReader jsonJobReader;
    private static ObjectReader jsonStudyReader;
    private static ObjectReader jsonSampleReader;
    private static Map<Class, ObjectReader> jsonReaderMap;

    static int getNewAutoIncrementId(MongoDBCollection metaCollection) {
        return getNewAutoIncrementId("idCounter", metaCollection);
    }

    static int getNewAutoIncrementId(String field, MongoDBCollection metaCollection){
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
        if(userId == null) {
            throw new CatalogDBException("userId param is null");
        }
        if(userId.equals("")) {
            throw new CatalogDBException("userId is empty");
        }

    }


    /*
    * Helper methods
    ********************/


    private User parseUser(QueryResult<DBObject> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonUserReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing user", e);
        }
    }

    private List<Study> parseStudies(QueryResult<DBObject> result) throws CatalogDBException {
        List<Study> studies = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                studies.add(jsonStudyReader.<Study>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing study", e);
        }
        return studies;
    }

    private File parseFile(QueryResult<DBObject> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonFileReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private List<File> parseFiles(QueryResult<DBObject> result) throws CatalogDBException {
        List<File> files = new LinkedList<>();
        try {
            for (DBObject o : result.getResult()) {
                files.add(jsonFileReader.<File>readValue(restoreDotsInKeys(o).toString()));
            }
            return files;
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private Job parseJob(QueryResult<DBObject> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonJobReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
    }

    private List<Job> parseJobs(QueryResult<DBObject> result) throws CatalogDBException {
        LinkedList<Job> jobs = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                jobs.add(jsonJobReader.<Job>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
        return jobs;
    }

    private List<Sample> parseSamples(QueryResult<DBObject> result) throws CatalogDBException {
        LinkedList<Sample> samples = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                samples.add(jsonSampleReader.<Sample>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing samples", e);
        }
        return samples;
    }

    private <T> List<T> parseObjects(QueryResult<DBObject> result, Class<T> tClass) throws CatalogDBException {
        LinkedList<T> objects = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                objects.add(jsonReaderMap.get(tClass).<T>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
        return objects;
    }

    private <T> T parseObject(QueryResult<DBObject> result, Class<T> tClass) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonReaderMap.get(tClass).readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
    }

    private DBObject getDbObject(Object object, String objectName) throws CatalogDBException {
        DBObject dbObject;
        try {
            dbObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(object));
            dbObject = replaceDotsInKeys(dbObject);
        } catch (Exception e) {
            throw new CatalogDBException("Error while writing to Json : " + objectName);
        }
        return dbObject;
    }

    static final String TO_REPLACE_DOTS = "&#46;";
//    static final String TO_REPLACE_DOTS = "\uff0e";

    /**
     * Scan all the DBObject and replace all the dots in keys with
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

    /**
     * Filter "include" and "exclude" options.
     *
     * Include and Exclude options are as absolute routes. This method removes all the values that are not in the
     * specified route. For the values in the route, the route is removed.
     *
     * [
     *  name,
     *  projects.id,
     *  projects.studies.id,
     *  projects.studies.alias,
     *  projects.studies.name
     * ]
     *
     * with route = "projects.studies.", then
     *
     * [
     *  id,
     *  alias,
     *  name
     * ]
     *
     * @param options
     * @param route
     * @return
     */
    private QueryOptions filterOptions(QueryOptions options, String route) {
        if(options == null) {
            return null;
        }

        QueryOptions filteredOptions = new QueryOptions(options); //copy queryOptions

        String[] filteringLists = {"include", "exclude"};
        for (String listName : filteringLists) {
            List<String> list = filteredOptions.getAsStringList(listName);
            List<String> filteredList = new LinkedList<>();
            int length = route.length();
            if(list != null) {
                for (String s : list) {
                    if(s.startsWith(route)) {
                        filteredList.add(s.substring(length));
                    }
                }
                filteredOptions.put(listName, filteredList);
            }
        }
        return filteredOptions;
    }


    /*  */

    private void addQueryStringListFilter(String key, QueryOptions options, DBObject query) {
        addQueryStringListFilter(key, options, key, query);
    }
    private void addQueryStringListFilter(String optionKey, QueryOptions options, String queryKey, DBObject query) {
        if (options.containsKey(optionKey)) {
            List<String> stringList = options.getAsStringList(optionKey);
            if (stringList.size() > 1) {
                query.put(queryKey, new BasicDBObject("$in", stringList));
            } else if (stringList.size() == 1) {
                query.put(queryKey, stringList.get(0));
            }
        }
    }

    private void addQueryIntegerListFilter(String key, QueryOptions options, DBObject query) {
        addQueryIntegerListFilter(key, options, key, query);
    }

    private void addQueryIntegerListFilter(String optionKey, QueryOptions options, String queryKey, DBObject query) {
        if (options.containsKey(optionKey)) {
            List<Integer> integerList = options.getAsIntegerList(optionKey);
            if (integerList.size() > 1) {
                query.put(queryKey, new BasicDBObject("$in", integerList));
            } else if (integerList.size() == 1) {
                query.put(queryKey, integerList.get(0));
            }
        }
    }

    /*  */

    private void filterStringParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedParams) {
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getString(s));
            }
        }
    }

    private void filterIntegerListParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedIntegerListParams) {
        for (String s : acceptedIntegerListParams) {
            if(parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getAsIntegerList(s));
            }
        }
    }

    private void filterMapParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedMapParams) {
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

    private void filterObjectParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedMapParams) {
        for (String s : acceptedMapParams) {
            if (parameters.containsKey(s)) {
                DBObject dbObject = null;
                try {
                    dbObject = getDbObject(parameters.get(s), s);
                    filteredParams.put(s , dbObject);
                } catch (CatalogDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void filterIntParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedIntParams) {
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if(anInt != Integer.MIN_VALUE) {
                    filteredParams.put(s, anInt);
                }
            }
        }
    }

    private void filterLongParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedLongParams) {
        for (String s : acceptedLongParams) {
            if(parameters.containsKey(s)) {
                long aLong = parameters.getLong(s, Long.MIN_VALUE);
                if (aLong != Long.MIN_VALUE) {
                    filteredParams.put(s, aLong);
                }
            }
        }
    }

}
