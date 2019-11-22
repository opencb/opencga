package org.opencb.opencga.server.rest.analysis;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.JacksonUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RestBodyParams {

    protected Map<String, String> dynamicParams;
    private final Map<String, Field> knownParams;

    public RestBodyParams(Map<String, String> dynamicParams) {
        this();
        this.dynamicParams = dynamicParams;
    }

    public RestBodyParams() {
        knownParams = Arrays.stream(this.getClass().getDeclaredFields())
                .filter(f -> !f.getName().equals("knownParams")) // Discard itself!
                .filter(f -> !f.getName().equals("dynamicParams")) // Discard itself!
                .collect(Collectors.toMap(Field::getName, Function.identity()));
    }

    public String toJson() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.PROTECTED_AND_PUBLIC);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        return objectMapper.writeValueAsString(this);
    }

    public ObjectMap toObjectMap() throws IOException {
        return new ObjectMap(toJson());
    }

    public Map<String, Object> toParams() throws IOException {
        ObjectMap objectMap = toObjectMap();
        objectMap.remove("dynamicParams");
        Map<String, Object> map = new HashMap<>(objectMap.size());
        addParams(map, objectMap);
        if (dynamicParams != null) {
            ObjectMap dynamicParams = new ObjectMap();
            dynamicParams.putAll(this.dynamicParams);
            addParams(map, dynamicParams);
        }
        return map;
    }

    public Map<String, Object> toParams(ObjectMap otherParams) throws IOException {
        Map<String, Object> map = toParams();
        addParams(map, otherParams);
        return map;
    }

    private void addParams(Map<String, Object> map, ObjectMap params) {
        for (String key : params.keySet()) {
            Field field = knownParams.get(key);
            String value = params.getString(key);
            if (StringUtils.isNotEmpty(value)) {
                if (field != null) {
                    // native boolean fields are "flags"
                    if (field.getType() == boolean.class) {
                        if (value.equals("true")) {
                            map.put(key, "");
                        }
                    } else {
                        map.put(key, value);
                    }
                } else {
                    map.put("-D" + key, value);
                }
            }
        }
    }
}
