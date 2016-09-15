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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Project {

    private long id;
    private String name;
    private String alias;
    private String creationDate;
    private String description;
    private String organization;
    private Status status;
    private String lastModified;
    private long diskUsage;

    private List<Study> studies;

    private Map<File.Bioformat, DataStore> dataStores;
    private Map<String, Object> attributes;


    public Project() {
    }

    public Project(String name, String alias, String description, Status status, String organization) {
        this(-1, name, alias, TimeUtils.getTime(), description, organization, status, null, 0, new LinkedList<>(),
                new HashMap<>(), new HashMap<>());
    }

    public Project(String name, String alias, String creationDate, String description, Status status, String lastModified,
                   long diskUsage, String organization) {
        this(-1, name, alias, creationDate, description, organization, status, lastModified, diskUsage, new LinkedList<>(),
                new HashMap<>(),  new HashMap<>());
    }

    public Project(long id, String name, String alias, String creationDate, String description, String organization, Status status,
                   String lastModified, long diskUsage, List<Study> studies, Map<File.Bioformat, DataStore> dataStores,
                   Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.creationDate = creationDate;
        this.description = description;
        this.organization = organization;
        this.status = status;
        this.lastModified = lastModified;
        this.diskUsage = diskUsage;
        this.studies = studies;
        this.dataStores = dataStores;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Project{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", status=").append(status);
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append(", diskUsage=").append(diskUsage);
        sb.append(", studies=").append(studies);
        sb.append(", dataStores=").append(dataStores);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Project setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Project setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public Project setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Project setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Project setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public Project setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Project setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public Project setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public Project setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
        return this;
    }

    public List<Study> getStudies() {
        return studies;
    }

    public Project setStudies(List<Study> studies) {
        this.studies = studies;
        return this;
    }

    public Map<File.Bioformat, DataStore> getDataStores() {
        return dataStores;
    }

    public Project setDataStores(Map<File.Bioformat, DataStore> dataStores) {
        this.dataStores = dataStores;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Project setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
