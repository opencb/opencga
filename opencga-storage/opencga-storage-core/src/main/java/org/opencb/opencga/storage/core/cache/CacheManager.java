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

package org.opencb.opencga.storage.core.cache;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.config.storage.CacheConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.RedissonClient;
import org.redisson.client.RedisConnectionException;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.codec.KryoCodec;
import org.redisson.core.RKeys;
import org.redisson.core.RMap;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by wasim on 26/10/16.
 */
public class CacheManager {

    private StorageConfiguration storageConfiguration;

    private Config redissonConfig;
    private Set<String> allowedTypesSet;
    private RedissonClient redissonClient;
    private boolean redisState;


    private static final String PREFIX_DATABASE_KEY = "ocga:";

    public CacheManager() {
    }

    public CacheManager(StorageConfiguration configuration) {

        CacheConfiguration cache;

        if (configuration != null && configuration.getCache() != null) {

            this.storageConfiguration = configuration;

            cache = configuration.getCache();
            redissonConfig = new Config();

            String host = (StringUtils.isNotEmpty(cache.getHost()))
                    ? cache.getHost()
                    : CacheConfiguration.DEFAULT_HOST;
            redissonConfig.useSingleServer().setAddress(host);

            String codec = (StringUtils.isNotEmpty(cache.getSerialization()))
                    ? cache.getSerialization()
                    : CacheConfiguration.DEFAULT_SERIALIZATION;

            this.allowedTypesSet = new HashSet<>(Arrays.asList(cache.getAllowedTypes().split(",")));


            if (StringUtils.isNotEmpty(cache.getPassword())) {
                redissonConfig.useSingleServer().setPassword(cache.getPassword());
            }

            if ("KRYO".equalsIgnoreCase(codec)) {
                redissonConfig.setCodec(new KryoCodec());
            } else {
                redissonConfig.setCodec(new JsonJacksonCodec());
            }

            redisState = true;

//            redissonClient = Redisson.create(redissonConfig);
            redissonClient = null;
        }
    }


    public <T> QueryResult<T> get(String key) {

        QueryResult<T> queryResult = new QueryResult<>();
        if (isActive()) {
            long start = System.currentTimeMillis();
            RMap<Integer, Map<String, Object>> map = getRedissonClient().getMap(key);

            try {
                // We only retrieve the first field of the HASH, which is the only one that exist.
                Map<Integer, Map<String, Object>> result = map.getAll(new HashSet<>(Collections.singletonList(0)));

                if (result != null && !result.isEmpty()) {
                    Object resultMap = result.get(0).get("result");
                    queryResult = (QueryResult<T>) resultMap;
                    queryResult.setDbTime((int) (System.currentTimeMillis() - start));
                }
            } catch (RedisConnectionException e) {
                redisState = false;
                queryResult.setWarningMsg("Unable to connect to Redis Cache, Please query WITHOUT Cache (Falling back to Database)");
                return queryResult;
            }
        }
        return queryResult;
    }

    public void set(String key, Query query, QueryResult queryResult) {

        if (isActive()) {
            if (queryResult.getDbTime() >= storageConfiguration.getCache().getSlowThreshold()
                    && queryResult.getResult().size() >= storageConfiguration.getCache().getMaxResultSize()) {
                RMap<Integer, Map<String, Object>> map = getRedissonClient().getMap(key);
                Map<String, Object> record = new HashMap<>();
                record.put("query", query);
                record.put("result", queryResult);
                try {
                    map.fastPut(0, record);
                } catch (RedisConnectionException e) {
                    redisState = false;
                    queryResult.setWarningMsg("Unable to connect to Redis Cache, Please query WITHOUT Cache (Falling back to Database)");
                }
            }
        }
    }

    public String createKey(String studyId, String allowedType, Query query, QueryOptions queryOptions) {

        queryOptions.remove("cache");
        queryOptions.remove("sId");

        StringBuilder key = new StringBuilder(PREFIX_DATABASE_KEY);
        key.append(studyId).append(":").append(allowedType);
        SortedMap<String, SortedSet<Object>> map = new TreeMap<>();

        for (String item : query.keySet()) {
            map.put(item.toLowerCase(), new TreeSet<>(query.getAsStringList(item)));
        }

        for (String item : queryOptions.keySet()) {
            map.put(item.toLowerCase(), new TreeSet<>(queryOptions.getAsStringList(item)));
        }

        String sha1 = DigestUtils.sha1Hex(map.toString());
        key.append(":").append(sha1);

        queryOptions.add("cache", "true");
        return key.toString();
    }

    public boolean isActive() {
        return storageConfiguration.getCache().isActive() && redisState;
    }

    public boolean isTypeAllowed(String type) {
        return allowedTypesSet.contains(type);
    }

    public void clear() {
        RKeys redisKeys = getRedissonClient().getKeys();
        redisKeys.deleteByPattern(PREFIX_DATABASE_KEY + "*");
    }

    public void clear(Pattern pattern) {
        RKeys redisKeys = getRedissonClient().getKeys();
        redisKeys.deleteByPattern(pattern.toString());
    }

    public void close() {
        if (redissonClient != null) {
            redissonClient.shutdown();
            redissonClient = null;
        }
    }

    private synchronized RedissonClient getRedissonClient() {
        if (redissonClient == null) {
            redissonClient = Redisson.create(redissonConfig);
        }
        return redissonClient;
    }

}
