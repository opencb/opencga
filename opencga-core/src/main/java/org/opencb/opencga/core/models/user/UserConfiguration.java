package org.opencb.opencga.core.models.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

public class UserConfiguration extends ObjectMap {

    private static final String FILTERS = "filters";
    private ObjectMapper objectMapper;
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
