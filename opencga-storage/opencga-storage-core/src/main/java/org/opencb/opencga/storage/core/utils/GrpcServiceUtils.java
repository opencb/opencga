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

package org.opencb.opencga.storage.core.utils;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 16/12/15.
 */
public class GrpcServiceUtils {


    /**
     * Create a gROC Managed Channel from a server host and port
     * @param serverUrl A string with the host and port, example: localhost:9091
     * @return MangedChannel object
     */
    public static ManagedChannel getManagedChannel(String serverUrl) {
        // We create the gRPC channel to the specified server host and port
        ManagedChannel channel = ManagedChannelBuilder.forTarget(serverUrl)
                .usePlaintext(true)
                .build();

        return channel;
    }

    public static Map<String, String> createMap(ObjectMap objectMap) {
        Map<String, String> map = new HashMap<>();
        for (String key : objectMap.keySet()) {
            map.put(key, objectMap.getString(key));
        }
        return map;
    }

    public static Query createQuery(Map<String, String> request) {
        Query query = new Query();
        for (String key : request.keySet()) {
            query.putIfNotNull(key, request.get(key));
        }
        return query;
    }

    public static QueryOptions createQueryOptions(Map<String, String> request) {
        QueryOptions queryOptions = new QueryOptions();
        for (String key : request.keySet()) {
            queryOptions.putIfNotNull(key, request.get(key));
        }
        return queryOptions;
    }

}
