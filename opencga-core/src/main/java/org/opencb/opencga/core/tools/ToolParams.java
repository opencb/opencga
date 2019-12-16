package org.opencb.opencga.core.tools;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class ToolParams {
    private final Map<String, Field> internalFieldsMap;

    public ToolParams() {
        internalFieldsMap = Arrays.stream(this.getClass().getFields())
                .filter(f -> !f.getName().equals("internalFieldsMap"))
                .collect(Collectors.toMap(Field::getName, Function.identity()));
    }

    public String toJson() {
        ObjectMapper objectMapper = getObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ObjectMap toObjectMap() {
        return new ObjectMap(toJson());
    }

    public ObjectMap toObjectMap(Map<String, ?> other) {
        return toObjectMap().appendAll(other);
    }

    public Map<String, Object> toParams() {
        ObjectMap objectMap = toObjectMap();
        Map<String, Object> map = new HashMap<>(objectMap.size());
        addParams(map, objectMap);
        return map;
    }

    public Map<String, Object> toParams(ObjectMap otherParams) {
        Map<String, Object> map = toParams();
        addParams(map, otherParams);
        return map;
    }


    public void updateParams(Map<String, Object> params) {
        ObjectMapper objectMapper = getObjectMapper();
        try {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.updateValue(this, params);
            params.putAll(this.toObjectMap());
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T fromParams(Class<T> clazz, Map<String, ?> params) {
        ObjectMapper objectMapper = getObjectMapper();
        return objectMapper.convertValue(params, clazz);
    }

    private static ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        return objectMapper;
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
