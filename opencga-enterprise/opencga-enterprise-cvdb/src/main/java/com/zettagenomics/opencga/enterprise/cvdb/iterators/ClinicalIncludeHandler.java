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

package com.zettagenomics.opencga.enterprise.cvdb.iterators;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.QueryOptions;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by jtarraga on 01/03/17.
 */
public class ClinicalIncludeHandler {

    public static final String INTERNAL_INCLUDE_MINIMUM_JSON = "internal-min";
    public static final String INTERNAL_INCLUDE_MEDIUM_JSON = "internal-medium";

    protected boolean exclude;
    protected List<String> inputFields;

    public ClinicalIncludeHandler(QueryOptions queryOptions) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            exclude = false;
            inputFields = new ArrayList<>(queryOptions.getAsStringList(QueryOptions.INCLUDE));
        } else if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            exclude = true;
            inputFields = new ArrayList<>(queryOptions.getAsStringList(QueryOptions.EXCLUDE));
        } else {
             inputFields = new ArrayList<>();
        }
        inputFields.remove(INTERNAL_INCLUDE_MINIMUM_JSON);
        inputFields.remove(INTERNAL_INCLUDE_MEDIUM_JSON);
    }

    public <T> T applyInclude(T object) {
        if (CollectionUtils.isEmpty(inputFields)) {
            return object;
        }
        return applyIncludeRecursive(object, inputFields);
    }

    private <T> T applyIncludeRecursive(T object, List<String> includes) {
        // Recursively collect fields from superclasses in order to get the inherited fields from superclasses or interfaces
        List<Field> allFields = new ArrayList<>();
        Class<?> clazz = object.getClass();
        while (clazz != null) {
            allFields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }

        for (Field field : allFields) {
            field.setAccessible(true);
            boolean toInclude;
            try {
                Object fieldValue = field.get(object);
                // Only set to null, field non-primitive and no-enum types
                // Primitive types: boolean, byte, char, short, int, long, float, and double
                // Enum types: Enum, EnumMap, EnumSet, Enumeration
                if (!field.getType().isPrimitive() && !isEnum(field.getType()) && field.getType() != Schema.class) {
                    if (isNested(field.getName(), includes)) {
                        // Field belonging to a nested object, e.g.: disorder.id
                        if (field.getType() == List.class && fieldValue instanceof List) {
                            // e.g.: primaryFindings.id, where primaryFindings is a list
                            List<?> list = (List<?>) fieldValue;
                            field.set(object, applyIncludeToList(list, updateIncludes(field.getName(), includes)));
                        } else {
                            // e.g.: disorder.id
                            if (fieldValue != null) {
                                field.set(object, applyIncludeRecursive(fieldValue, updateIncludes(field.getName(), includes)));
                            }
                        }
                    } else {
                        // Field belonging to the current object, e.g.: id
                        if (isIncluded(field.getName(), includes)) {
                            toInclude = !exclude;
                        } else {
                            toInclude = exclude;
                        }
                        if (!toInclude) {
                            try {
                                field.set(object, null);
                            } catch (Exception e) {
                                System.out.println("Impossible to set to null the field '" + field.getName() + "', value = " + object);
                            }
                        }
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return object;
    }

    private boolean isEnum(Class<?> type) {
        if (type == Enum.class || type == EnumSet.class || type == EnumMap.class || type == Enumeration.class) {
            return true;
        }
        return false;
    }

    private <T> List<T> applyIncludeToList(List<T> list, List<String> includes) {
        List<T> newList = new ArrayList<>();
        for (T item : list) {
            newList.add(applyIncludeRecursive(item, includes));
        }
        return newList;
    }

    private boolean isIncluded(String name, List<String> includeList) {
        if (includeList.contains(name)) {
            return true;
        }
        // Check for nested field names, e.g.: disorder.id
        for (String include : includeList) {
            if (include.contains(".")) {
                String[] split = include.split("\\.");
                if (name.equals(split[0])) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isNested(String name, List<String> includeList) {
        for (String include : includeList) {
            // Check for nested field names, e.g.: disorder.id
            if (include.contains(".")) {
                String[] split = include.split("\\.");
                if (name.equals(split[0])) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<String> updateIncludes(String name, List<String> includeList) {
        List<String> updatedIncludeList = new ArrayList<>();
        for (String include : includeList) {
            if (include.contains(".")) {
                String[] split = include.split("\\.");
                if (name.equals(split[0])) {
                    // Update include list, e.g.: disorder.id -> id
                    updatedIncludeList.add(include.substring(include.indexOf(".") + 1));
                }
            }
        }
        return updatedIncludeList;
    }

    public List<String> getInputFields() {
        return inputFields;
    }

    public ClinicalIncludeHandler setInputFields(List<String> inputFields) {
        this.inputFields = inputFields;
        return this;
    }
}
