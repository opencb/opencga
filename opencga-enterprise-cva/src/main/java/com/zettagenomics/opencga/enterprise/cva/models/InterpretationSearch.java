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

package com.zettagenomics.opencga.enterprise.cva.models;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.List;

public class InterpretationSearch {

    @Field("id")
    private String id;

    @Field("description")
    private String description;

    @Field("clinicalAnalysisId")
    private String clinicalAnalysisId;

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
    private String analystDate;

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
    private String statusDate;

    @Field("creationDate")
    private String creationDate;

    @Field("modificationDate")
    private String modificationDate;

    @Field("version")
    private int version;

    // Interpreation stored in a JSON string
    @Field("json")
    private String json;

    public InterpretationSearch() {
        init();
    }

    private void init() {
        panelIds = new ArrayList<>();
        methodDependencies = new ArrayList<>();
        comments = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationSearch{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
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

    public String getId() {
        return id;
    }

    public InterpretationSearch setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public InterpretationSearch setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public InterpretationSearch setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public List<String> getPanelIds() {
        return panelIds;
    }

    public InterpretationSearch setPanelIds(List<String> panelIds) {
        this.panelIds = panelIds;
        return this;
    }

    public String getAnalystId() {
        return analystId;
    }

    public InterpretationSearch setAnalystId(String analystId) {
        this.analystId = analystId;
        return this;
    }

    public String getAnalystName() {
        return analystName;
    }

    public InterpretationSearch setAnalystName(String analystName) {
        this.analystName = analystName;
        return this;
    }

    public String getAnalystEmail() {
        return analystEmail;
    }

    public InterpretationSearch setAnalystEmail(String analystEmail) {
        this.analystEmail = analystEmail;
        return this;
    }

    public String getAnalystAssignedBy() {
        return analystAssignedBy;
    }

    public InterpretationSearch setAnalystAssignedBy(String analystAssignedBy) {
        this.analystAssignedBy = analystAssignedBy;
        return this;
    }

    public String getAnalystDate() {
        return analystDate;
    }

    public InterpretationSearch setAnalystDate(String analystDate) {
        this.analystDate = analystDate;
        return this;
    }

    public String getMethodName() {
        return methodName;
    }

    public InterpretationSearch setMethodName(String methodName) {
        this.methodName = methodName;
        return this;
    }

    public String getMethodVersion() {
        return methodVersion;
    }

    public InterpretationSearch setMethodVersion(String methodVersion) {
        this.methodVersion = methodVersion;
        return this;
    }

    public String getMethodCommit() {
        return methodCommit;
    }

    public InterpretationSearch setMethodCommit(String methodCommit) {
        this.methodCommit = methodCommit;
        return this;
    }

    public List<String> getMethodDependencies() {
        return methodDependencies;
    }

    public InterpretationSearch setMethodDependencies(List<String> methodDependencies) {
        this.methodDependencies = methodDependencies;
        return this;
    }

    public List<String> getComments() {
        return comments;
    }

    public InterpretationSearch setComments(List<String> comments) {
        this.comments = comments;
        return this;
    }

    public boolean isLocked() {
        return locked;
    }

    public InterpretationSearch setLocked(boolean locked) {
        this.locked = locked;
        return this;
    }

    public String getStatusId() {
        return statusId;
    }

    public InterpretationSearch setStatusId(String statusId) {
        this.statusId = statusId;
        return this;
    }

    public String getStatusName() {
        return statusName;
    }

    public InterpretationSearch setStatusName(String statusName) {
        this.statusName = statusName;
        return this;
    }

    public String getStatusDescription() {
        return statusDescription;
    }

    public InterpretationSearch setStatusDescription(String statusDescription) {
        this.statusDescription = statusDescription;
        return this;
    }

    public String getStatusDate() {
        return statusDate;
    }

    public InterpretationSearch setStatusDate(String statusDate) {
        this.statusDate = statusDate;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public InterpretationSearch setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public InterpretationSearch setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public InterpretationSearch setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getJson() {
        return json;
    }

    public InterpretationSearch setJson(String json) {
        this.json = json;
        return this;
    }
}


