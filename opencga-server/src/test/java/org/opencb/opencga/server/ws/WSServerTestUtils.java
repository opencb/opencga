package org.opencb.opencga.server.ws;

import org.opencb.datastore.core.QueryResponse;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by ralonso on 9/25/14.
 */
public class WSServerTestUtils {

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
