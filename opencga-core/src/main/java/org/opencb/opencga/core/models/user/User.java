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

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.project.Project;

import java.util.*;

/**
 * Created by imedina on 11/09/14.
 */
@DataClass(id = "User", since = "1.0",
        description = "User data model hosts information about any user.")
public class User {

    /**
     * User ID is a mandatory parameter when creating a new sample, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */
    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "name", indexed = true, description = FieldConstants.USER_NAME)
    private String name;

    @DataField(id = "email", indexed = true, description = FieldConstants.USER_EMAIL)
    private String email;

    @DataField(id = "organization", indexed = true, description = FieldConstants.USER_ORGANIZATION)
    private String organization;

    @DataField(id = "creationDate", since = "3.2.1", description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", since = "3.2.1", description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "account", description = FieldConstants.USER_ACCOUNT)
    @Deprecated
    private Account account;

    @DataField(id = "internal", indexed = true, description = FieldConstants.GENERIC_INTERNAL)
    private UserInternal internal;

    @DataField(id = "quota", indexed = true, description = FieldConstants.USER_QUOTA)
    private UserQuota quota;

    /**
     * A List with related projects.
     *
     * @apiNote
     */
    @DataField(id = "projects", indexed = true, description = FieldConstants.USER_PROJECTS)
    private List<Project> projects;

    @DataField(id = "configs", indexed = true, description = FieldConstants.USER_CONFIGS)
    private Map<String, ObjectMap> configs;

    @DataField(id = "filters", indexed = true, description = FieldConstants.USER_FILTERS)
    private List<UserFilter> filters;

    /**
     * You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.
     *
     * @apiNote
     */
    @DataField(id = "attributes", indexed = true, description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public User() {
    }

    public User(String id) {
        this(id, id, null, null, null, null, new UserInternal(new UserStatus()), null, Collections.emptyMap(),
                new LinkedList<>(), Collections.emptyMap());
    }

    public User(String id, String name, String email, String organization, UserInternal internal) {
        this(id, name, email, organization, null, null, internal, null, new HashMap<>(), new LinkedList<>(), new HashMap<>());
    }

    public User(String id, String name, String email, String organization, UserInternal internal, UserQuota quota, List<Project> projects,
                Map<String, ObjectMap> configs, List<UserFilter> filters, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.organization = organization;
        this.internal = internal;
        this.quota = quota;
        this.projects = projects;
        this.configs = configs;
        this.filters = filters;
        this.attributes = attributes;
    }

    public User(String id, String name, String email, String organization, String creationDate, String modificationDate,
                UserInternal internal, UserQuota quota, Map<String, ObjectMap> configs, List<UserFilter> filters,
                Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.organization = organization;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.internal = internal;
        this.quota = quota;
        this.configs = configs;
        this.filters = filters;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("User{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", internal=").append(internal);
        sb.append(", quota=").append(quota);
        sb.append(", projects=").append(projects);
        sb.append(", configs=").append(configs);
        sb.append(", filters=").append(filters);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
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

    public String getOrganization() {
        return organization;
    }

    public User setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public User setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public User setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    @Deprecated
    public Account getAccount() {
        return account;
    }

    @Deprecated
    public User setAccount(Account account) {
        this.account = account;
        return this;
    }

    public UserInternal getInternal() {
        return internal;
    }

    public User setInternal(UserInternal internal) {
        this.internal = internal;
        return this;
    }

    public UserQuota getQuota() {
        return quota;
    }

    public User setQuota(UserQuota quota) {
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

    public Map<String, ObjectMap> getConfigs() {
        return configs;
    }

    public User setConfigs(Map<String, ObjectMap> configs) {
        this.configs = configs;
        return this;
    }

    public List<UserFilter> getFilters() {
        return filters;
    }

    public User setFilters(List<UserFilter> filters) {
        this.filters = filters;
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
