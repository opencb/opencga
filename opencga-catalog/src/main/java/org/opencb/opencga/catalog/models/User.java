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

    /**
     * This specifies the role of this user in OpenCGA, possible values: admin, user, demo, ...
     */
    private Role role;
    private Status status;
    private String lastActivity;
    private long diskUsage;
    private long diskQuota;

    private List<Project> projects;
    private List<Tool> tools;

    /**
     * Open and closed sessions for this user.
     * More than one session can be open, i.e. logged from Chrome and Firefox
     */
    private List<Session> sessions;

    private Map<String, Object> configs;
    private Map<String, Object> attributes;

    public User() {
    }

    public User(String id, String name, String email, String password, String organization, Role role, Status status) {
        this(id, name, email, password, organization, role, status, "", -1, -1, new ArrayList<Project>(),
                new ArrayList<Tool>(0), new ArrayList<Session>(0),
                new HashMap<String, Object>(), new HashMap<String, Object>());
    }

    public User(String id, String name, String email, String password, String organization, Role role, Status status,
                String lastActivity, long diskUsage, long diskQuota, List<Project> projects, List<Tool> tools,
                List<Session> sessions, Map<String, Object> configs, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.password = password;
        this.organization = organization;
        this.role = role;
        this.status = status;
        this.lastActivity = lastActivity;
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
        sb.append(", role=").append(role);
        sb.append(", status='").append(status).append('\'');
        sb.append(", lastActivity='").append(lastActivity).append('\'');
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getLastActivity() {
        return lastActivity;
    }

    public void setLastActivity(String lastActivity) {
        this.lastActivity = lastActivity;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public long getDiskQuota() {
        return diskQuota;
    }

    public void setDiskQuota(long diskQuota) {
        this.diskQuota = diskQuota;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
    }

    public List<Tool> getTools() {
        return tools;
    }

    public void setTools(List<Tool> tools) {
        this.tools = tools;
    }

    public List<Session> getSessions() {
        return sessions;
    }

    public void setSessions(List<Session> sessions) {
        this.sessions = sessions;
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String, Object> configs) {
        this.configs = configs;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    /*
     * Things to think about:
     * private List<Credential> credentials = new ArrayList<Credential>();
     * private List<Bucket> buckets = new ArrayList<Bucket>();
     */

    public enum Role {
        ADMIN,  //= "admin";
        USER,  //= "user";
        ANONYMOUS  //= "anonymous";
    }

}
