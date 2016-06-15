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

import org.opencb.opencga.catalog.models.acls.FileAcl;
import org.opencb.opencga.core.common.TimeUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class File {

    /* Attributes known values */
    @Deprecated
    public static final String DELETE_DATE = "deleteDate";      //Long

    private long id;
    /**
     * File name.
     */
    private String name;
    /**
     * Formats: file, folder, index.
     */
    private Type type;
    /**
     * Formats: txt, executable, image, ...
     */
    private Format format;
    /**
     * BAM, VCF, ...
     */
    private Bioformat bioformat;
    /**
     * Optional external file location. If null, file is inside its study.
     */
    private URI uri;
    private String path;
    private String ownerId;
    private String creationDate;
    private String modificationDate;
    private String description;
    private FileStatus status;
//    @Deprecated
    //private FileStatusEnum fileStatusEnum;
    private long diskUsage;
    //private long studyId;
    private long experimentId;
    private List<Long> sampleIds;
    /**
     * This field values -1 when file has been uploaded.
     */
    private long jobId;
    private List<FileAcl> acls;
    private Index index;
    @Deprecated
    private List<AnnotationSet> annotationSets;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    /*
     * To think
     * ACL, url,  responsible,  extended source ??
     */

    public File() {
    }

    public File(String name, Type type, Format format, Bioformat bioformat, String path, String ownerId,
                String description, FileStatus status, long diskUsage) {
        this(-1, name, type, format, bioformat, path, ownerId, TimeUtils.getTime(), description, status, diskUsage,
                -1, new LinkedList<>(), -1, new LinkedList<>(), new HashMap<String, Object>(),
                new HashMap<>());
    }

    public File(String name, Type type, Format format, Bioformat bioformat, String path, String ownerId,
                String creationDate, String description, FileStatus status, long diskUsage) {
        this(-1, name, type, format, bioformat, path, ownerId, creationDate, description, status, diskUsage,
                -1, new LinkedList<>(), -1, new LinkedList<>(), new HashMap<String, Object>(),
                new HashMap<>());
    }

    public File(long id, String name, Type type, Format format, Bioformat bioformat, String path, String ownerId,
                String creationDate, String description, FileStatus status, long diskUsage, long experimentId,
                List<Long> sampleIds, long jobId, List<FileAcl> acls, Map<String, Object> stats,
                Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.format = format;
        this.bioformat = bioformat;
        this.uri = null;
        this.path = path;
        this.ownerId = ownerId;
        this.creationDate = creationDate;
        this.modificationDate = creationDate;
        this.description = description;
        this.status = status;
        this.diskUsage = diskUsage;
        this.experimentId = experimentId;
        this.sampleIds = sampleIds;
        this.jobId = jobId;
        this.acls = acls;
        this.index = null;
        this.annotationSets = new LinkedList<>();
        this.stats = stats;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("File{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", format=").append(format);
        sb.append(", bioformat=").append(bioformat);
        sb.append(", uri=").append(uri);
        sb.append(", path='").append(path).append('\'');
        sb.append(", ownerId='").append(ownerId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", status=").append(status);
        sb.append(", diskUsage=").append(diskUsage);
        sb.append(", experimentId=").append(experimentId);
        sb.append(", sampleIds=").append(sampleIds);
        sb.append(", jobId=").append(jobId);
        sb.append(", acls=").append(acls);
        sb.append(", index=").append(index);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", stats=").append(stats);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public Bioformat getBioformat() {
        return bioformat;
    }

    public void setBioformat(Bioformat bioformat) {
        this.bioformat = bioformat;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public void setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /*public FileStatusEnum getFileStatusEnum() {
        return fileStatusEnum;
    }

    public void setFileStatusEnum(FileStatusEnum fileStatusEnum) {
        this.fileStatusEnum = fileStatusEnum;
    }
*/
    public FileStatus getStatus() {
        return status;
    }

    public void setStatus(FileStatus status) {
        this.status = status;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public long getExperimentId() {
        return experimentId;
    }

    public void setExperimentId(long experimentId) {
        this.experimentId = experimentId;
    }

    public List<Long> getSampleIds() {
        return sampleIds;
    }

    public void setSampleIds(List<Long> sampleIds) {
        this.sampleIds = sampleIds;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public List<FileAcl> getAcls() {
        return acls;
    }

    public File setAcls(List<FileAcl> acls) {
        this.acls = acls;
        return this;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public void setStats(Map<String, Object> stats) {
        this.stats = stats;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public static class FileStatus extends Status {

        public static final String STAGE = "STAGE";
        public static final String MISSING = "MISSING";
//        public static final String TRASHED = "TRASHED";

        public FileStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public FileStatus(String status) {
            this(status, "");
        }

        public FileStatus() {
            this(READY, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (status != null && (status.equals(STAGE) || status.equals(MISSING))) {
                return true;
            }
            return false;
        }
    }

    public enum Type {
        FOLDER,
        FILE
    }


    public enum Compression {
        GZIP,
        BGZIP,
        ZIP,
        SNAPPY,
        NONE,
    }

    /**
     * General format of the file, such as text, or binary, etc.
     */
    public enum Format {

        VCF,
        BCF,
        GVCF,
        TBI,

        SAM,
        BAM,
        BAI,
        CRAM,
        FASTQ,
        PED,

        TAB_SEPARATED_VALUES, COMMA_SEPARATED_VALUES, XML, PROTOCOL_BUFFER, JSON, AVRO, PARQUET, //Serialization formats

        IMAGE,
        PLAIN,
        BINARY,
        EXECUTABLE,
        @Deprecated GZIP,
        UNKNOWN,
    }

    /**
     * Specific format of the biological file, such as variant, alignment, pedigree, etc.
     */
    public enum Bioformat {
        MICROARRAY_EXPRESSION_ONECHANNEL_AGILENT,
        MICROARRAY_EXPRESSION_ONECHANNEL_AFFYMETRIX,
        MICROARRAY_EXPRESSION_ONECHANNEL_GENEPIX,
        MICROARRAY_EXPRESSION_TWOCHANNELS_AGILENT,
        MICROARRAY_EXPRESSION_TWOCHANNELS_GENEPIX,
        DATAMATRIX_EXPRESSION,
        //        DATAMATRIX_SNP,
//        IDLIST_GENE,
//        IDLIST_TRANSCRIPT,
//        IDLIST_PROTEIN,
//        IDLIST_SNP,
//        IDLIST_FUNCTIONALTERMS,
//        IDLIST_RANKED,
        IDLIST,
        IDLIST_RANKED,
        ANNOTATION_GENEVSANNOTATION,

        OTHER_NEWICK,
        OTHER_BLAST,
        OTHER_INTERACTION,
        OTHER_GENOTYPE,
        OTHER_PLINK,
        OTHER_VCF,
        OTHER_PED,

        @Deprecated VCF4,

        VARIANT,
        ALIGNMENT,
        SEQUENCE,
        PEDIGREE,
        NONE
    }
}
