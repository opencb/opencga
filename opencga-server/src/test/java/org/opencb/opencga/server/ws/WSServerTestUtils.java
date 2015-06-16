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

package org.opencb.opencga.server.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by ralonso on 9/25/14.
 */
public class WSServerTestUtils {

    static ObjectMapper mapper = OpenCGAWSServer.jsonObjectMapper;

    public static <T> QueryResponse<QueryResult<T>> parseResult(String json, Class<T> clazz) throws IOException {
        ObjectReader reader = mapper.reader(mapper.getTypeFactory().constructParametricType(
                QueryResponse.class, mapper.getTypeFactory().constructParametricType(QueryResult.class, clazz)));
        return reader.readValue(json);
    }

    public static String getField(QueryResponse queryResponse, String field){
        //            queryResponse = objectMapper.readValue(response, QueryResponse.class);

        ArrayList coll = (ArrayList) queryResponse.getResponse();
        LinkedHashMap linkedHashMap = (LinkedHashMap)coll.get(0);
        ArrayList arrayList = (ArrayList)linkedHashMap.get("result");
        String[] firstElement = arrayList.get(0).toString().replace("{","").replace("}","").split(",");
        for (int i = 0; i < firstElement.length; i++) {
            String[] tuple = firstElement[i].split("=");
//            System.out.println("tuple[0] = " + tuple[0]);
//            System.out.println("tuple[1] = " + tuple[1]);
            String fieldInTuple = new String(tuple[0]);
            fieldInTuple = fieldInTuple.trim();
            if(fieldInTuple.equalsIgnoreCase(field)) {
                String valueInTuple = new String(tuple[1]);
                return valueInTuple;
            }
        }
        return "";
    }
}
