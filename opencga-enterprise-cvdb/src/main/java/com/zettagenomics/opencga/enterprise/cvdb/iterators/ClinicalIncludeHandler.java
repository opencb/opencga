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

import org.apache.commons.collections4.CollectionUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jtarraga on 01/03/17.
 */
public class ClinicalIncludeHandler {

    protected List<String> includeList;

    public ClinicalIncludeHandler() {
    }

    public ClinicalIncludeHandler(List<String> includeList) {
        this.includeList = includeList;
    }

    public <T> T applyInclude(T object) {
        if (CollectionUtils.isEmpty(includeList)) {
            return object;
        }
        return applyIncludeRecursive(object, includeList);
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
            boolean toInclude = isIncluded(field.getName(), includes);
            try {
                // Only set to null, field non-primitive types
                // Primitive types are boolean, byte, char, short, int, long, float, and double
                if (!field.getType().isPrimitive()) {
                    if (isNested(field.getName(), includes)) {
                        // Field belonging to a nested object, e.g.: disorder.id
                        Object nestedObject = field.get(object);
                        field.set(object, applyIncludeRecursive(nestedObject, updateIncludes(field.getName(), includes)));
                    } else {
                        // Field belonging to the current object, e.g.: id
                        if (!toInclude) {
                            field.set(object, null);
                        }
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return object;
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

    public List<String> getIncludeList() {
        return includeList;
    }

    public ClinicalIncludeHandler setIncludeList(List<String> includeList) {
        this.includeList = includeList;
        return this;
    }
}
