package org.opencb.opencga.storage.core.local.models;

import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;

import java.nio.file.Path;
import java.util.Map;

/**
 * Created by pfurio on 24/11/16.
 */
public class StudyInfo {

    private String sessionId;
    private String userId;
    private long projectId;
    private long studyId;

    private String projectAlias;
    private String studyAlias;

    private Path workspace;
    private Map<File.Bioformat, DataStore> dataStores;

    private FileInfo fileInfo;

    public StudyInfo() {
        this.projectId = -1;
        this.studyId = -1;
        this.fileInfo = new FileInfo();
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
        return studyId;
    }

    public StudyInfo setStudyId(long studyId) {
        this.studyId = studyId;
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
        return studyAlias;
    }

    public StudyInfo setStudyAlias(String studyAlias) {
        this.studyAlias = studyAlias;
        return this;
    }

    public Path getWorkspace() {
        return workspace;
    }

    public StudyInfo setWorkspace(Path workspace) {
        this.workspace = workspace;
        return this;
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
        return fileInfo;
    }

    public StudyInfo setFileInfo(FileInfo fileInfo) {
        this.fileInfo = fileInfo;
        return this;
    }
}
