/*
 * Copyright 2015 OpenCB
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private long diskUsage;
    private long diskQuota;

    private List<Project> projects;
    private List<Tool> tools;

    /**
     * Open sessions for this user. Closed are stored in audit
     * More than one session can be open, i.e. logged from CLI and Web browsers
     */
    private List<Session> sessions;

    private Map<String, Object> configs;
    private Map<String, Object> attributes;


    public User() {
    }

    public User(String id, String name, String email, String password, String organization, String status) {
        this(id, name, email, password, organization, null, status, "", -1, -1, new ArrayList<>(), new ArrayList<>(0), new ArrayList<>(0),
                new HashMap<>(), new HashMap<>());
    }

    public User(String id, String name, String email, String password, String organization, Account account, String status,
                String lastModified, long diskUsage, long diskQuota, List<Project> projects, List<Tool> tools, List<Session> sessions,
                Map<String, Object> configs, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.password = password;
        this.organization = organization;
        // FIXME: Account should always be passed and not null
        this.account = account != null ? account : new Account();
        this.status = new UserStatus(status);
        this.lastModified = lastModified;
        this.diskUsage = diskUsage;
        this.diskQuota = diskQuota;
        this.projects = projects;
        this.tools = tools;
        this.sessions = sessions;
        this.configs = configs;
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
        sb.append(", diskUsage=").append(diskUsage);
        sb.append(", diskQuota=").append(diskQuota);
        sb.append(", projects=").append(projects);
        sb.append(", tools=").append(tools);
        sb.append(", sessions=").append(sessions);
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

    public long getDiskUsage() {
        return diskUsage;
    }

    public User setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
        return this;
    }

    public long getDiskQuota() {
        return diskQuota;
    }

    public User setDiskQuota(long diskQuota) {
        this.diskQuota = diskQuota;
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

    public List<Session> getSessions() {
        return sessions;
    }

    public User setSessions(List<Session> sessions) {
        this.sessions = sessions;
        return this;
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    public User setConfigs(Map<String, Object> configs) {
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
