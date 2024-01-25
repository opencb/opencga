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
import java.util.List;

public class ClinicalAnalysisSearch {

    // Catalog fields

    @Field("studyId")
    private String studyId;

    @Field("viewers")
    private List<String> viewers;

    // "Primary" and "foreign" keys

    @Field("id")
    private String id;

    // Clinical analysis fields

    @Field("description")
    private String description;

    @Field("type")
    private String type;

    @Field("disorderId")
    private String disorderId;

    @Field("fileNames")
    private List<String> fileNames;

    @Field("probandId")
    private String probandId;

    @Field("familyId")
    private String familyId;

    @Field("familyPhenotypeNames")
    private List<String> familyPhenotypeNames;

    @Field("familyMemberIds")
    private List<String> familyMemberIds;

    @Field("panelIds")
    private List<String> panelIds;

    @Field("report")
    private String report;

    @Field("status")
    private String status;

    @Field("locked")
    private boolean locked;

    @Field("liteJson")
    private String liteJson;

    @Field("fullJson")
    private String fullJson;

    public ClinicalAnalysisSearch() {
        viewers = new ArrayList<>();
        fileNames = new ArrayList<>();
        familyPhenotypeNames = new ArrayList<>();
        familyMemberIds = new ArrayList<>();
        panelIds = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisSearch{");
        sb.append("studyId='").append(studyId).append('\'');
        sb.append(", viewers=").append(viewers);
        sb.append(", id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", disorderId='").append(disorderId).append('\'');
        sb.append(", fileNames=").append(fileNames);
        sb.append(", probandId='").append(probandId).append('\'');
        sb.append(", familyId='").append(familyId).append('\'');
        sb.append(", familyPhenotypeNames=").append(familyPhenotypeNames);
        sb.append(", familyMemberIds=").append(familyMemberIds);
        sb.append(", panelIds=").append(panelIds);
        sb.append(", report='").append(report).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", locked=").append(locked);
        sb.append(", liteJson='").append(liteJson).append('\'');
        sb.append(", fullJson='").append(fullJson).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getStudyId() {
        return studyId;
    }

    public ClinicalAnalysisSearch setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public List<String> getViewers() {
        return viewers;
    }

    public ClinicalAnalysisSearch setViewers(List<String> viewers) {
        this.viewers = viewers;
        return this;
    }

    public String getId() {
        return id;
    }

    public ClinicalAnalysisSearch setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalAnalysisSearch setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getType() {
        return type;
    }

    public ClinicalAnalysisSearch setType(String type) {
        this.type = type;
        return this;
    }

    public String getDisorderId() {
        return disorderId;
    }

    public ClinicalAnalysisSearch setDisorderId(String disorderId) {
        this.disorderId = disorderId;
        return this;
    }

    public List<String> getFileNames() {
        return fileNames;
    }

    public ClinicalAnalysisSearch setFileNames(List<String> fileNames) {
        this.fileNames = fileNames;
        return this;
    }

    public String getProbandId() {
        return probandId;
    }

    public ClinicalAnalysisSearch setProbandId(String probandId) {
        this.probandId = probandId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public ClinicalAnalysisSearch setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public List<String> getFamilyPhenotypeNames() {
        return familyPhenotypeNames;
    }

    public ClinicalAnalysisSearch setFamilyPhenotypeNames(List<String> familyPhenotypeNames) {
        this.familyPhenotypeNames = familyPhenotypeNames;
        return this;
    }

    public List<String> getFamilyMemberIds() {
        return familyMemberIds;
    }

    public ClinicalAnalysisSearch setFamilyMemberIds(List<String> familyMemberIds) {
        this.familyMemberIds = familyMemberIds;
        return this;
    }

    public List<String> getPanelIds() {
        return panelIds;
    }

    public ClinicalAnalysisSearch setPanelIds(List<String> panelIds) {
        this.panelIds = panelIds;
        return this;
    }

    public String getReport() {
        return report;
    }

    public ClinicalAnalysisSearch setReport(String report) {
        this.report = report;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public ClinicalAnalysisSearch setStatus(String status) {
        this.status = status;
        return this;
    }

    public boolean isLocked() {
        return locked;
    }

    public ClinicalAnalysisSearch setLocked(boolean locked) {
        this.locked = locked;
        return this;
    }

    public String getLiteJson() {
        return liteJson;
    }

    public ClinicalAnalysisSearch setLiteJson(String liteJson) {
        this.liteJson = liteJson;
        return this;
    }

    public String getFullJson() {
        return fullJson;
    }

    public ClinicalAnalysisSearch setFullJson(String fullJson) {
        this.fullJson = fullJson;
        return this;
    }
}


