/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.catalog.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Created by imedina on 11/09/14.
 */
public class User {

    /**
     * id is a unique string in the database.
     */
    private String id;
    private String name;
    private String email;
    private String password;
    private String organization;

    private Account account;

    private UserStatus status;
    private String lastModified;
    private long size;
    private long quota;

    private List<Project> projects;
    private List<Tool> tools;

    private UserConfiguration configs;
    private Map<String, Object> attributes;

    public static class UserConfiguration extends ObjectMap {

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

        public List<Filter> getFilters() {
            Object object = get(FILTERS);
            if (object == null) {
                return new LinkedList<>();
            }
            if (isListFilters(object)) {
                return (List<Filter>) object;
            } else {
                //convert with objectMapper
                List<Filter> filters = new ArrayList<>();
                try {
                    if (objectMapper == null) {
                        objectMapper = new ObjectMapper();
                        objectReader = objectMapper.readerFor(Filter.class);
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

        public UserConfiguration setFilters(List<Filter> filters) {
            put(FILTERS, filters);
            return this;
        }

        private boolean isListFilters(Object object) {
            if (object instanceof List) {
                List list = (List) object;
                if (!list.isEmpty()) {
                    if (list.get(0) instanceof Filter) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            return false;
        }

    }

    public static class Filter {
        private String name;
        private String description;
        private File.Bioformat bioformat;
        private Query query;
        private QueryOptions options;

        public Filter() {
        }

        public Filter(String name, String description, File.Bioformat bioformat, Query query, QueryOptions options) {
            this.name = name;
            this.description = description;
            this.bioformat = bioformat;
            this.query = query;
            this.options = options;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Filter{");
            sb.append("name='").append(name).append('\'');
            sb.append(", description='").append(description).append('\'');
            sb.append(", bioformat=").append(bioformat);
            sb.append(", query=").append(query);
            sb.append(", options=").append(options);
            sb.append('}');
            return sb.toString();
        }

        public String getName() {
            return name;
        }

        public Filter setName(String name) {
            this.name = name;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public Filter setDescription(String description) {
            this.description = description;
            return this;
        }

        public File.Bioformat getBioformat() {
            return bioformat;
        }

        public Filter setBioformat(File.Bioformat bioformat) {
            this.bioformat = bioformat;
            return this;
        }

        public Query getQuery() {
            return query;
        }

        public Filter setQuery(Query query) {
            this.query = query;
            return this;
        }

        public QueryOptions getOptions() {
            return options;
        }

        public Filter setOptions(QueryOptions options) {
            this.options = options;
            return this;
        }
    }

    public User() {
    }

    public User(String id, String name, String email, String password, String organization, String status) {
        this(id, name, email, password, organization, null, status, "", -1, -1, new ArrayList<>(),
                new ArrayList<>(0), new HashMap<>(), new HashMap<>());
    }

    public User(String id, String name, String email, String password, String organization, Account account, String status,
                String lastModified, long size, long quota, List<Project> projects, List<Tool> tools, Map<String, Object> configs,
                Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.password = password;
        this.organization = organization;
        // FIXME: Account should always be passed and not null
        this.account = account != null ? account : new Account();
        this.status = new UserStatus(status);
        this.lastModified = lastModified;
        this.size = size;
        this.quota = quota;
        this.projects = projects;
        this.tools = tools;
        if (configs == null) {
            this.configs = new UserConfiguration();
        } else {
            this.configs = new UserConfiguration(configs);
        }
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("User{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", account=").append(account);
        sb.append(", status=").append(status);
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append(", size=").append(size);
        sb.append(", quota=").append(quota);
        sb.append(", projects=").append(projects);
        sb.append(", tools=").append(tools);
        sb.append(", configs=").append(configs);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public static class UserStatus extends Status {

        public static final String BANNED = "BANNED";

        public UserStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public UserStatus(String status) {
            this(status, "");
        }

        public UserStatus() {
            this(READY, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (status != null && (status.equals(BANNED))) {
                return true;
            }
            return false;
        }
    }


    public String getId() {
        return id;
    }

    public User setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public User setName(String name) {
        this.name = name;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public User setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public User setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public User setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public Account getAccount() {
        return account;
    }

    public User setAccount(Account account) {
        this.account = account;
        return this;
    }

    public UserStatus getStatus() {
        return status;
    }

    public User setStatus(UserStatus status) {
        this.status = status;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public User setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    public long getSize() {
        return size;
    }

    public User setSize(long size) {
        this.size = size;
        return this;
    }

    public long getQuota() {
        return quota;
    }

    public User setQuota(long quota) {
        this.quota = quota;
        return this;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public User setProjects(List<Project> projects) {
        this.projects = projects;
        return this;
    }

    public List<Tool> getTools() {
        return tools;
    }

    public User setTools(List<Tool> tools) {
        this.tools = tools;
        return this;
    }

    public UserConfiguration getConfigs() {
        return configs;
    }

    public User setConfigs(UserConfiguration configs) {
        this.configs = configs;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public User setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
