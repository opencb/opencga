package org.opencb.opencga.server.rest.analysis;

import org.opencb.commons.datastore.core.ObjectMap;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class RestBodyParams {

    public Map<String, String> dynamicParams;
    private final Map<String, Field> knownParams;

    public RestBodyParams() {
        knownParams = Arrays.stream(this.getClass().getFields())
                .filter(f -> !f.getName().equals("knownParams")) // Discard itself!
                .collect(Collectors.toMap(Field::getName, Function.identity()));
    }

    public Map<String, String> toParams() throws IOException {
        ObjectMap objectMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
        HashMap<String, String> map = new HashMap<>(objectMap.size());
        addParams(map, objectMap);
        if (dynamicParams != null) {
            ObjectMap dynamicParams = new ObjectMap();
            dynamicParams.putAll(this.dynamicParams);
            addParams(map, dynamicParams);
        }
        return map;
    }

    public Map<String, String> toParams(ObjectMap otherParams) throws IOException {
        Map<String, String> map = toParams();
        addParams(map, otherParams);
        return map;
    }

    private void addParams(Map<String, String> map, ObjectMap params) {
        for (String key : params.keySet()) {
            Field field = knownParams.get(key);
            if (field != null) {
                // native boolean fields are "flags"
                if (field.getType() == boolean.class) {
                    if (params.getString(key).equals("true")) {
                        map.put(key, "");
                    }
                } else {
                    map.put(key, params.getString(key));
                }
            } else {
                map.put("-D" + key, params.getString(key));
            }
        }
    }
}
