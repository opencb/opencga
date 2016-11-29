package org.opencb.opencga.storage.core.local.models;

import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 24/11/16.
 */
public class StudyInfo {

    private String sessionId;
    private String userId;
    private long projectId;
    private Study study;

    private String projectAlias;

    private Path workspace;
    private Map<File.Bioformat, DataStore> dataStores;

    private List<FileInfo> fileInfos;

    public StudyInfo() {
        this.projectId = -1;
        this.fileInfos = new ArrayList<>();
    }

    public String getUserId() {
        return userId;
    }

    public StudyInfo setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public long getProjectId() {
        return projectId;
    }

    public StudyInfo setProjectId(long projectId) {
        this.projectId = projectId;
        return this;
    }

    public long getStudyId() {
        return study != null ? study.getId() : -1;
    }

    public Study getStudy() {
        return study;
    }

    public StudyInfo setStudy(Study study) {
        this.study = study;
        return this;
    }

    public String getProjectAlias() {
        return projectAlias;
    }

    public StudyInfo setProjectAlias(String projectAlias) {
        this.projectAlias = projectAlias;
        return this;
    }

    public String getStudyAlias() {
        return study != null ? study.getAlias() : null;
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
}
