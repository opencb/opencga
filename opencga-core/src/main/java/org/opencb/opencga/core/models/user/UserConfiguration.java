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

package org.opencb.opencga.core.models.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class UserConfiguration extends ObjectMap {

    private static final String FILTERS = "filters";
    @DataField(description = ParamConstants.USER_CONFIGURATION_OBJECT_MAPPER_DESCRIPTION)
    private ObjectMapper objectMapper;
    @DataField(description = ParamConstants.USER_CONFIGURATION_OBJECT_READER_DESCRIPTION)
    private ObjectReader objectReader;

    public UserConfiguration() {
        this(new HashMap<>());
    }

    public UserConfiguration(Map<String, Object> map) {
        super(map);
        put(FILTERS, new ArrayList<>());
    }

    public List<UserFilter> getFilters() {
        Object object = get(FILTERS);
        if (object == null) {
            return new LinkedList<>();
        }
        if (isListFilters(object)) {
            return (List<UserFilter>) object;
        } else {
            //convert with objectMapper
            List<UserFilter> filters = new ArrayList<>();
            try {
                if (objectMapper == null) {
                    objectMapper = new ObjectMapper();
                    objectReader = objectMapper.readerFor(UserFilter.class);
                }
                for (Object filterObject : ((List) object)) {
                    filters.add(objectReader.readValue(objectMapper.writeValueAsString(filterObject)));
                }
                setFilters(filters);
                return filters;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public UserConfiguration setFilters(List<UserFilter> filters) {
        put(FILTERS, filters);
        return this;
    }

    private boolean isListFilters(Object object) {
        if (object instanceof List) {
            List list = (List) object;
            if (!list.isEmpty()) {
                if (list.get(0) instanceof UserFilter) {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }

}
