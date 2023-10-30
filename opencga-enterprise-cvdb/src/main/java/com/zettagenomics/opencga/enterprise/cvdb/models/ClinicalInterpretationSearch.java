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

package com.zettagenomics.opencga.enterprise.cvdb.models;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ClinicalInterpretationSearch {

    // Catalog fields

    @Field("studyId")
    private String studyId;

    @Field("studyJson")
    private String studyJson;

    // "Primary" and "foreign" keys

    @Field("id")
    private String id;

    @Field("caId")
    private String caId;

    // Clinical interpretation fields

    @Field("primary")
    private boolean primary;

    @Field("description")
    private String description;

    // Panel IDs contain both IDs and names
    @Field("panelIds")
    private List<String> panelIds;

    @Field("analystId")
    private String analystId;

    @Field("analystName")
    private String analystName;

    @Field("analystEmail")
    private String analystEmail;

    @Field("analystAssignedBy")
    private String analystAssignedBy;

    @Field("analystDate")
    private Date analystDate;

    @Field("methodName")
    private String methodName;

    @Field("methodVersion")
    private String methodVersion;

    @Field("methodCommit")
    private String methodCommit;

	// Method software/dependencies are stores: name -- version
    @Field("methodDependencies")
    private List<String> methodDependencies;

	// Comments are stores: author -- message -- tag1:tag2:.. -- date -- -->
    @Field("comments")
    private List<String> comments;

    @Field("locked")
    private boolean locked;

    @Field("statusId")
    private String statusId;

    @Field("statusName")
    private String statusName;

    @Field("statusDescription")
    private String statusDescription;

    @Field("statusDate")
    private Date statusDate;

    @Field("creationDate")
    private Date creationDate;

    @Field("modificationDate")
    private Date modificationDate;

    @Field("version")
    private int version;

    // Interpreation stored in a JSON string
    @Field("json")
    private String json;

    public ClinicalInterpretationSearch() {
        init();
    }

    private void init() {
        panelIds = new ArrayList<>();
        methodDependencies = new ArrayList<>();
        comments = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalInterpretationSearch{");
        sb.append("studyId='").append(studyId).append('\'');
        sb.append(", studyJson='").append(studyJson).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", caId='").append(caId).append('\'');
        sb.append(", primary=").append(primary);
        sb.append(", description='").append(description).append('\'');
        sb.append(", panelIds=").append(panelIds);
        sb.append(", analystId='").append(analystId).append('\'');
        sb.append(", analystName='").append(analystName).append('\'');
        sb.append(", analystEmail='").append(analystEmail).append('\'');
        sb.append(", analystAssignedBy='").append(analystAssignedBy).append('\'');
        sb.append(", analystDate='").append(analystDate).append('\'');
        sb.append(", methodName='").append(methodName).append('\'');
        sb.append(", methodVersion='").append(methodVersion).append('\'');
        sb.append(", methodCommit='").append(methodCommit).append('\'');
        sb.append(", methodDependencies=").append(methodDependencies);
        sb.append(", comments=").append(comments);
        sb.append(", locked=").append(locked);
        sb.append(", statusId='").append(statusId).append('\'');
        sb.append(", statusName='").append(statusName).append('\'');
        sb.append(", statusDescription='").append(statusDescription).append('\'');
        sb.append(", statusDate='").append(statusDate).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", version=").append(version);
        sb.append(", json='").append(json).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getStudyId() {
        return studyId;
    }

    public ClinicalInterpretationSearch setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getStudyJson() {
        return studyJson;
    }

    public ClinicalInterpretationSearch setStudyJson(String studyJson) {
        this.studyJson = studyJson;
        return this;
    }

    public String getId() {
        return id;
    }

    public ClinicalInterpretationSearch setId(String id) {
        this.id = id;
        return this;
    }

    public String getCaId() {
        return caId;
    }

    public ClinicalInterpretationSearch setCaId(String caId) {
        this.caId = caId;
        return this;
    }

    public boolean isPrimary() {
        return primary;
    }

    public ClinicalInterpretationSearch setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalInterpretationSearch setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<String> getPanelIds() {
        return panelIds;
    }

    public ClinicalInterpretationSearch setPanelIds(List<String> panelIds) {
        this.panelIds = panelIds;
        return this;
    }

    public String getAnalystId() {
        return analystId;
    }

    public ClinicalInterpretationSearch setAnalystId(String analystId) {
        this.analystId = analystId;
        return this;
    }

    public String getAnalystName() {
        return analystName;
    }

    public ClinicalInterpretationSearch setAnalystName(String analystName) {
        this.analystName = analystName;
        return this;
    }

    public String getAnalystEmail() {
        return analystEmail;
    }

    public ClinicalInterpretationSearch setAnalystEmail(String analystEmail) {
        this.analystEmail = analystEmail;
        return this;
    }

    public String getAnalystAssignedBy() {
        return analystAssignedBy;
    }

    public ClinicalInterpretationSearch setAnalystAssignedBy(String analystAssignedBy) {
        this.analystAssignedBy = analystAssignedBy;
        return this;
    }

    public Date getAnalystDate() {
        return analystDate;
    }

    public ClinicalInterpretationSearch setAnalystDate(Date analystDate) {
        this.analystDate = analystDate;
        return this;
    }

    public String getMethodName() {
        return methodName;
    }

    public ClinicalInterpretationSearch setMethodName(String methodName) {
        this.methodName = methodName;
        return this;
    }

    public String getMethodVersion() {
        return methodVersion;
    }

    public ClinicalInterpretationSearch setMethodVersion(String methodVersion) {
        this.methodVersion = methodVersion;
        return this;
    }

    public String getMethodCommit() {
        return methodCommit;
    }

    public ClinicalInterpretationSearch setMethodCommit(String methodCommit) {
        this.methodCommit = methodCommit;
        return this;
    }

    public List<String> getMethodDependencies() {
        return methodDependencies;
    }

    public ClinicalInterpretationSearch setMethodDependencies(List<String> methodDependencies) {
        this.methodDependencies = methodDependencies;
        return this;
    }

    public List<String> getComments() {
        return comments;
    }

    public ClinicalInterpretationSearch setComments(List<String> comments) {
        this.comments = comments;
        return this;
    }

    public boolean isLocked() {
        return locked;
    }

    public ClinicalInterpretationSearch setLocked(boolean locked) {
        this.locked = locked;
        return this;
    }

    public String getStatusId() {
        return statusId;
    }

    public ClinicalInterpretationSearch setStatusId(String statusId) {
        this.statusId = statusId;
        return this;
    }

    public String getStatusName() {
        return statusName;
    }

    public ClinicalInterpretationSearch setStatusName(String statusName) {
        this.statusName = statusName;
        return this;
    }

    public String getStatusDescription() {
        return statusDescription;
    }

    public ClinicalInterpretationSearch setStatusDescription(String statusDescription) {
        this.statusDescription = statusDescription;
        return this;
    }

    public Date getStatusDate() {
        return statusDate;
    }

    public ClinicalInterpretationSearch setStatusDate(Date statusDate) {
        this.statusDate = statusDate;
        return this;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public ClinicalInterpretationSearch setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Date getModificationDate() {
        return modificationDate;
    }

    public ClinicalInterpretationSearch setModificationDate(Date modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public ClinicalInterpretationSearch setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getJson() {
        return json;
    }

    public ClinicalInterpretationSearch setJson(String json) {
        this.json = json;
        return this;
    }
}


