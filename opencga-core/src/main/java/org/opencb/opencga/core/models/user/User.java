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

package org.opencb.opencga.core.models.user;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.project.Project;

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
    private String organization;

    private Account account;

    private UserInternal internal;
    private UserQuota quota;

    private List<Project> projects;

    private Map<String, ObjectMap> configs;
    private List<UserFilter> filters;
    private Map<String, Object> attributes;

    public User() {
    }

    public User(String id, Account account) {
        this(id, id, null, null, account, new UserInternal(new UserStatus()), null, Collections.emptyList(), Collections.emptyMap(),
                new LinkedList<>(), Collections.emptyMap());
    }

    public User(String id, String name, String email, String organization, UserInternal internal) {
        this(id, name, email, organization, null, internal, null, new ArrayList<>(), new HashMap<>(), new LinkedList<>(), new HashMap<>());
    }

    public User(String id, String name, String email, String organization, Account account, UserInternal internal, UserQuota quota,
                List<Project> projects, Map<String, ObjectMap> configs, List<UserFilter> filters, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.organization = organization;
        this.account = account != null ? account : new Account();
        this.internal = internal;
        this.quota = quota;
        this.projects = projects;
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
        sb.append(", account=").append(account);
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

    public Account getAccount() {
        return account;
    }

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
