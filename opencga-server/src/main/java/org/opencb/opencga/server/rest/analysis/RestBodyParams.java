package org.opencb.opencga.server.rest.analysis;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RestBodyParams {
    private final Map<String, Field> internalFieldsMap;

    public RestBodyParams() {
        internalFieldsMap = Arrays.stream(this.getClass().getFields())
                .filter(f -> !f.getName().equals("internalFieldsMap"))
                .collect(Collectors.toMap(Field::getName, Function.identity()));
    }

    public String toJson() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.PROTECTED_AND_PUBLIC);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        return objectMapper.writeValueAsString(this);
    }

    public ObjectMap toObjectMap() throws IOException {
        return new ObjectMap(toJson());
    }

    public Map<String, Object> toParams() throws IOException {
        ObjectMap objectMap = toObjectMap();
        Map<String, Object> map = new HashMap<>(objectMap.size());
        addParams(map, objectMap);
        return map;
    }

    public Map<String, Object> toParams(ObjectMap otherParams) throws IOException {
        Map<String, Object> map = toParams();
        addParams(map, otherParams);
        return map;
    }

    private void addParams(Map<String, Object> map, ObjectMap params) {
        for (String key : params.keySet()) {
            Field field = internalFieldsMap.get(key);
            String value = params.getString(key);
            if (StringUtils.isNotEmpty(value)) {
                // native boolean fields are "flags"
                if (field != null && field.getType() == boolean.class) {
                    if (value.equals("true")) {
                        map.put(key, "");
                    }
                } else if (field != null &&  Map.class.isAssignableFrom(field.getType())) {
                    map.put(key, params.getMap(key));
                } else {
                    map.put(key, value);
                }
            }
        }
    }
}
