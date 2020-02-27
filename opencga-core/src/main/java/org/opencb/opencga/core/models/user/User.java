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

import org.opencb.opencga.core.models.common.Status;
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

    private UserStatus status;
    private UserQuota quota;

    private List<Project> projects;

    private UserConfiguration configs;
    private Map<String, Object> attributes;

    public User() {
    }

    public User(String id, Account account) {
        this(id, id, null, null, account, Status.READY, null, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
    }

    public User(String id, String name, String email, String organization, String status) {
        this(id, name, email, organization, null, status, null, new ArrayList<>(), new HashMap<>(), new HashMap<>());
    }

    public User(String id, String name, String email, String organization, Account account, String status, UserQuota quota,
                List<Project> projects, Map<String, Object> configs, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.organization = organization;
        this.account = account != null ? account : new Account();
        this.status = new UserStatus(status);
        this.quota = quota;
        this.projects = projects;
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
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", account=").append(account);
        sb.append(", status=").append(status);
        sb.append(", quota=").append(quota);
        sb.append(", projects=").append(projects);
        sb.append(", configs=").append(configs);
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

    public UserStatus getStatus() {
        return status;
    }

    public User setStatus(UserStatus status) {
        this.status = status;
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
