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

package org.opencb.opencga.core.tools;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class ToolParams {
    private Map<String, Class<?>> internalPropertiesMap = null;

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
        ObjectMap objectMap = toObjectMap();
        if (other != null) {
            objectMap.appendAll(other);
        }
        return objectMap;
    }

    public Map<String, Object> toParams() {
        ObjectMap objectMap = toObjectMap();
        Map<String, Object> map = new HashMap<>(objectMap.size());
        addParams(map, objectMap);
        return map;
    }

    public Map<String, Object> toParams(String key, String value) {
        return toParams(new ObjectMap(key, value));
    }

    public Map<String, Object> toParams(ObjectMap otherParams) {
        Map<String, Object> map = toParams();
        addParams(map, otherParams);
        return map;
    }


    public void updateParams(Map<String, Object> params) {
        ObjectMapper objectMapper = getObjectMapper();
        try {

            // Split string lists
            ObjectMap copy = new ObjectMap(params);
            for (Map.Entry<String, Class<?>> entry : loadPropertiesMap().entrySet()) {
                String key = entry.getKey();
                if (params.containsKey(key)) {
                    Class<?> type = entry.getValue();

                    if (Collection.class.isAssignableFrom(type)) {
                        copy.put(key, copy.getAsStringList(key));
                    } else if (boolean.class == type) {
                        Object value = copy.get(key);
                        if (value instanceof String && ((String) value).isEmpty()) {
                            copy.put(key, true);
                        } else {
                            copy.put(key, copy.getBoolean(key));
                        }
                    } else {
                        Object value = copy.get(key);
                        if (Collection.class.isAssignableFrom(value.getClass())) {
                            copy.put(key, copy.getString(key));
                        }
                    }
                }
            }
            objectMapper.updateValue(this, copy);

            params.putAll(this.toObjectMap());
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T extends ToolParams> T fromParams(Class<T> clazz, Map<String, ?> params) {
        try {
            T t = clazz.newInstance();
            t.updateParams(new HashMap<>(params));
            return t;
        } catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private static ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }

    private void addParams(Map<String, Object> map, ObjectMap params) {
        loadPropertiesMap();
        for (String key : params.keySet()) {
            Class<?> fieldClass = internalPropertiesMap.get(key);
            String value = params.getString(key);
            if (StringUtils.isNotEmpty(value)) {
                // native boolean fields are "flags"
                if (fieldClass == boolean.class) {
                    if (value.equals("true")) {
                        map.put(key, "");
                    }
                } else if (fieldClass != null
                        && (Map.class.isAssignableFrom(fieldClass) || ToolParams.class.isAssignableFrom(fieldClass))) {
                    map.put(key, params.getMap(key));
                } else {
                    map.put(key, value);
                }
            }
        }
    }

    public Map<String, Class<?>> fields() {
        return Collections.unmodifiableMap(loadPropertiesMap());
    }

    private Map<String, Class<?>> loadPropertiesMap() {
        if (internalPropertiesMap == null) {
            internalPropertiesMap = buildPropertiesMap(this.getClass());
        }
        return internalPropertiesMap;
    }

    private static Map<String, Class<?>> buildPropertiesMap(Class<? extends ToolParams> aClass) {
        ObjectMapper objectMapper = getObjectMapper();
        BeanDescription beanDescription = objectMapper.getSerializationConfig().introspect(objectMapper.constructType(aClass));
        Map<String, Class<?>> internalPropertiesMap = new HashMap<>(beanDescription.findProperties().size());
        for (BeanPropertyDefinition property : beanDescription.findProperties()) {
            Class<?> rawPrimaryType = property.getRawPrimaryType();
            internalPropertiesMap.put(property.getName(), rawPrimaryType);
            if (ToolParams.class.isAssignableFrom(rawPrimaryType)) {
                if (hasNestedFields(((Class<? extends ToolParams>) rawPrimaryType))) {
                    throw new IllegalStateException("Invalid param '" + property.getName() + "' from ToolParams " + aClass + ". "
                            + "Invalid multiple level nesting params");
                }
            }
        }
        return internalPropertiesMap;
    }

    private boolean hasNestedFields() {
        return hasNestedFields(loadPropertiesMap());
    }

    private static boolean hasNestedFields(Class<? extends ToolParams> aClass) {
        return hasNestedFields(buildPropertiesMap(aClass));
    }

    private static boolean hasNestedFields(Map<String, Class<?>> propertiesMap) {
        for (Class<?> fieldClass : propertiesMap.values()) {
            if (Map.class.isAssignableFrom(fieldClass) || ToolParams.class.isAssignableFrom(fieldClass)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return getClass().getName() + toObjectMap().toJson();
    }
}
