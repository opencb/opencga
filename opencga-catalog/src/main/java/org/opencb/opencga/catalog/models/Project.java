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

import org.opencb.opencga.core.common.TimeUtils;

import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Project {

    private int id;
    private String name;
    private String alias;
    private String creationDate;
    private String description;
    private String organization;
    private String status;
    private String lastActivity;
    private long diskUsage;

    private List<AclEntry> acl;
    private List<Study> studies;

    private Map<File.Bioformat, DataStore> dataStores;
    private Map<String, Object> attributes;

    public Project() {
    }

    public Project(String name, String alias, String description, String status, String organization) {
        this(-1, name, alias, TimeUtils.getTime(), description, organization, status, null, 0,
                new LinkedList<AclEntry>(), new LinkedList<Study>(), new HashMap<String, Object>());
    }
    public Project(String name, String alias, String creationDate, String description, String status,
                   String lastActivity, long diskUsage, String organization) {
        this(-1, name, alias, creationDate, description, organization, status, lastActivity, diskUsage,
                new LinkedList<AclEntry>(), new LinkedList<Study>(), new HashMap<String, Object>());
    }

    public Project(int id, String name, String alias, String creationDate, String description, String organization,
                   String status, String lastActivity, long diskUsage, List<AclEntry> acl, List<Study> studies,
                   Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.creationDate = creationDate;
        this.description = description;
        this.organization = organization;
        this.status = status;
        this.lastActivity = lastActivity;
        this.diskUsage = diskUsage;
        this.acl = acl;
        this.studies = studies;
        this.dataStores = new HashMap<>();
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Project{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", alias='" + alias + '\'' +
                ", creationDate='" + creationDate + '\'' +
                ", description='" + description + '\'' +
                ", organization='" + organization + '\'' +
                ", status='" + status + '\'' +
                ", lastActivity='" + lastActivity + '\'' +
                ", diskUsage=" + diskUsage +
                ", acl=" + acl +
                ", studies=" + studies +
                ", attributes=" + attributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
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

    public List<AclEntry> getAcl() {
        return acl;
    }

    public void setAcl(List<AclEntry> acl) {
        this.acl = acl;
    }

    public List<Study> getStudies() {
        return studies;
    }

    public void setStudies(List<Study> studies) {
        this.studies = studies;
    }

    public Map<File.Bioformat, DataStore> getDataStores() {
        return dataStores;
    }

    public void setDataStores(Map<File.Bioformat, DataStore> dataStores) {
        this.dataStores = dataStores;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }
}
