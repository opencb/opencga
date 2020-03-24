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

package org.opencb.opencga.analysis.models;

import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.Study;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 24/11/16.
 */
@Deprecated
public class StudyInfo {

    private String sessionId;
    private String userId;
    private long projectUid;
    private Study study;

    private String projectId;

    private Path workspace;
    private Map<File.Bioformat, DataStore> dataStores;
    private ProjectOrganism organism;

    private List<FileInfo> fileInfos;

    public StudyInfo() {
        this.projectUid = -1;
        this.fileInfos = new ArrayList<>();
    }

    public String getUserId() {
        return userId;
    }

    public StudyInfo setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public long getProjectUid() {
        return projectUid;
    }

    public StudyInfo setProjectUid(long projectUid) {
        this.projectUid = projectUid;
        return this;
    }

    public long getStudyUid() {
        return study != null ? study.getUid() : -1;
    }

    public Study getStudy() {
        return study;
    }

    public StudyInfo setStudy(Study study) {
        this.study = study;
        return this;
    }

    public String getProjectId() {
        return projectId;
    }

    public StudyInfo setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public String getStudyFQN() {
        return study != null ? study.getFqn() : null;
    }


    public Path getWorkspace() {
        return study != null ? Paths.get(study.getUri().getRawPath()) : null;
    }

    public Map<File.Bioformat, DataStore> getDataStores() {
        return dataStores;
    }

    public StudyInfo setDataStores(Map<File.Bioformat, DataStore> dataStores) {
        this.dataStores = dataStores;
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public StudyInfo setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public FileInfo getFileInfo() {
        return fileInfos.get(0);
    }

    public StudyInfo setFileInfo(FileInfo fileInfo) {
        this.fileInfos.set(0, fileInfo);
        return this;
    }

    public List<FileInfo> getFileInfos() {
        return fileInfos;
    }

    public StudyInfo setFileInfos(List<FileInfo> fileInfos) {
        this.fileInfos = fileInfos;
        return this;
    }

    public ProjectOrganism getOrganism() {
        return organism;
    }

    public StudyInfo setOrganism(ProjectOrganism organism) {
        this.organism = organism;
        return this;
    }
}
