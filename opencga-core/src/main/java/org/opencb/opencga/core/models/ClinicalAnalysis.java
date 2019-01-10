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

package org.opencb.opencga.core.models;

import org.opencb.biodata.models.clinical.interpretation.Comment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysis extends PrivateStudyUid {

    private String id;
    @Deprecated
    private String name;
    private String uuid;
    private String description;
    private Type type;

    private Disorder disorder;

    // Map of sample id, list of files (VCF, BAM and BIGWIG)
    private Map<String, List<File>> files;

    private Individual proband;
    private Family family;
    private List<Interpretation> interpretations;

    private Priority priority;

    private String creationDate;
    private String modificationDate;
    private String dueDate;
    private ClinicalStatus status;
    private int release;

    private List<Comment> comments;
    private Map<String, Object> attributes;

    public enum Priority {
        URGENT, HIGH, MEDIUM, LOW
    }

    public enum Type {
        SINGLE, DUO, TRIO, FAMILY, AUTO, MULTISAMPLE
    }

    // Todo: Think about a better place to have this enum
    @Deprecated
    public enum Action {
        ADD,
        SET,
        REMOVE
    }

    public static class ClinicalStatus extends Status {

        public static final String WAIT = "WAIT";
        public static final String REJECTED = "REJECTED";
        public static final String READY_FOR_INTERPRETATION = "READY_FOR_INTERPRETATION";
        public static final String INTERPRETATION_IN_PROGRESS = "INTERPRETATION_IN_PROGRESS";
        public static final String INTERPRETED = "INTERPRETED";
        public static final String PENDING_REVIEW = "PENDING_REVIEW";
        public static final String READY_FOR_REPORT = "READY_FOR_REPORT";
        public static final String REPORT_IN_PROGRESS = "REPORT_IN_PROGRESS";
        public static final String DONE = "DONE";
        public static final String REVIEW = "REVIEW";
        public static final String CLOSED = "CLOSED";

        public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, WAIT, REJECTED, READY_FOR_INTERPRETATION,
                INTERPRETATION_IN_PROGRESS, INTERPRETED, PENDING_REVIEW, READY_FOR_REPORT, REPORT_IN_PROGRESS, DONE, REVIEW, CLOSED);

        public ClinicalStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public ClinicalStatus(String status) {
            this(status, "");
        }

        public ClinicalStatus() {
            this(WAIT, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (STATUS_LIST.contains(status)) {
                return true;
            }
            return false;
        }
    }

    public ClinicalAnalysis() {
    }

    public ClinicalAnalysis(String id, String description, Type type, Disorder disorder, Map<String, List<File>> files,
                            Individual proband, Family family, List<Interpretation> interpretations, Priority priority, String creationDate,
                            String dueDate, List<Comment> comments, ClinicalStatus status, int release, Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.interpretations = interpretations;
        this.priority = priority;
        this.creationDate = creationDate;
        this.dueDate = dueDate;
        this.comments = comments;
        this.status = status;
        this.release = release;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysis{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", disorder=").append(disorder);
        sb.append(", files=").append(files);
        sb.append(", proband=").append(proband);
        sb.append(", family=").append(family);
        sb.append(", interpretations=").append(interpretations);
        sb.append(", priority=").append(priority);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", comments=").append(comments);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public ClinicalAnalysis setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public ClinicalAnalysis setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public ClinicalAnalysis setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public ClinicalAnalysis setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalAnalysis setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalAnalysis setDescription(String description) {
        this.description = description;
        return this;
    }

    public Type getType() {
        return type;
    }

    public ClinicalAnalysis setType(Type type) {
        this.type = type;
        return this;
    }

    public Disorder getDisorder() {
        return disorder;
    }

    public ClinicalAnalysis setDisorder(Disorder disorder) {
        this.disorder = disorder;
        return this;
    }

    public Map<String, List<File>> getFiles() {
        return files;
    }

    public ClinicalAnalysis setFiles(Map<String, List<File>> files) {
        this.files = files;
        return this;
    }

    public Individual getProband() {
        return proband;
    }

    public ClinicalAnalysis setProband(Individual proband) {
        this.proband = proband;
        return this;
    }

    public Family getFamily() {
        return family;
    }

    public ClinicalAnalysis setFamily(Family family) {
        this.family = family;
        return this;
    }

    public List<Interpretation> getInterpretations() {
        return interpretations;
    }

    public ClinicalAnalysis setInterpretations(List<Interpretation> interpretations) {
        this.interpretations = interpretations;
        return this;
    }

    public Priority getPriority() {
        return priority;
    }

    public ClinicalAnalysis setPriority(Priority priority) {
        this.priority = priority;
        return this;
    }

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalAnalysis setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ClinicalAnalysis setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public ClinicalAnalysis setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public ClinicalAnalysis setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }

    public ClinicalStatus getStatus() {
        return status;
    }

    public ClinicalAnalysis setStatus(ClinicalStatus status) {
        this.status = status;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public ClinicalAnalysis setRelease(int release) {
        this.release = release;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalAnalysis setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
