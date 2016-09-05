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

import org.opencb.opencga.catalog.models.acls.AbstractAcl;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.common.TimeUtils;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class File extends AbstractAcl<FileAclEntry> {

    private long id;
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

    private URI uri;
    private String path;

    private String creationDate;
    @Deprecated
    private String modificationDate;

    private String description;
    private FileStatus status;
    private boolean external;

    private long diskUsage;
    private long experimentId;
    private List<Long> sampleIds;


    /**
     * This field values -1 when file has been uploaded.
     */
    private long jobId;
    private List<RelatedFile> relatedFiles;

//    private List<FileAclEntry> acl;
    private FileIndex index;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;


    public File() {
    }

    public File(String name, Type type, Format format, Bioformat bioformat, String path, String description, FileStatus status,
                long diskUsage) {
        this(-1, name, type, format, bioformat, null, path, TimeUtils.getTime(), TimeUtils.getTime(), description, status, false,
                diskUsage, -1, Collections.emptyList(), -1, Collections.emptyList(), Collections.emptyList(), null, Collections.emptyMap(),
                Collections.emptyMap());
    }

    public File(long id, String name, Type type, Format format, Bioformat bioformat, URI uri, String path, String creationDate,
                String modificationDate, String description, FileStatus status, boolean external, long diskUsage, long experimentId,
                List<Long> sampleIds, long jobId, List<RelatedFile> relatedFiles, List<FileAclEntry> acl, FileIndex index,
                Map<String, Object> stats, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.format = format;
        this.bioformat = bioformat;
        this.uri = uri;
        this.path = path;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.description = description;
        this.status = status;
        this.external = external;
        this.diskUsage = diskUsage;
        this.experimentId = experimentId;
        this.sampleIds = sampleIds;
        this.jobId = jobId;
        this.relatedFiles = relatedFiles;
        this.acl = acl;
        this.index = index != null ? index : new FileIndex();
        this.stats = stats;
        this.attributes = attributes;
    }

    public static class FileStatus extends Status {

        public static final String STAGE = "STAGE";
        public static final String MISSING = "MISSING";
        public static final String PENDING_DELETE = "PENDING_DELETE";
        public static final String DELETING = "DELETING"; // This status is set exactly before deleting the file from disk.
        public static final String REMOVED = "REMOVED";

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
        FILE,
        DIRECTORY,
        @Deprecated
        FOLDER
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

    public static class RelatedFile {

        private long fileId;
        private Relation relation;

        public enum Relation {
            PRODUCED_FROM,
            PART_OF_PAIR,
            PEDIGREE;
        }

        public RelatedFile() {
        }

        public RelatedFile(long fileId, Relation relation) {
            this.fileId = fileId;
            this.relation = relation;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RelatedFile{");
            sb.append("fileId=").append(fileId);
            sb.append(", relation=").append(relation);
            sb.append('}');
            return sb.toString();
        }

        public long getFileId() {
            return fileId;
        }

        public RelatedFile setFileId(long fileId) {
            this.fileId = fileId;
            return this;
        }

        public Relation getRelation() {
            return relation;
        }

        public RelatedFile setRelation(Relation relation) {
            this.relation = relation;
            return this;
        }
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
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", status=").append(status);
        sb.append(", external=").append(external);
        sb.append(", diskUsage=").append(diskUsage);
        sb.append(", experimentId=").append(experimentId);
        sb.append(", sampleIds=").append(sampleIds);
        sb.append(", jobId=").append(jobId);
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append(", acl=").append(acl);
        sb.append(", index=").append(index);
        sb.append(", stats=").append(stats);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public File setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public File setName(String name) {
        this.name = name;
        return this;
    }

    public Type getType() {
        return type;
    }

    public File setType(Type type) {
        this.type = type;
        return this;
    }

    public Format getFormat() {
        return format;
    }

    public File setFormat(Format format) {
        this.format = format;
        return this;
    }

    public Bioformat getBioformat() {
        return bioformat;
    }

    public File setBioformat(Bioformat bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public URI getUri() {
        return uri;
    }

    public File setUri(URI uri) {
        this.uri = uri;
        return this;
    }

    public String getPath() {
        return path;
    }

    public File setPath(String path) {
        this.path = path;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public File setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public File setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public File setDescription(String description) {
        this.description = description;
        return this;
    }

    public FileStatus getStatus() {
        return status;
    }

    public File setStatus(FileStatus status) {
        this.status = status;
        return this;
    }

    public boolean isExternal() {
        return external;
    }

    public File setExternal(boolean external) {
        this.external = external;
        return this;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public File setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
        return this;
    }

    public long getExperimentId() {
        return experimentId;
    }

    public File setExperimentId(long experimentId) {
        this.experimentId = experimentId;
        return this;
    }

    public List<Long> getSampleIds() {
        return sampleIds;
    }

    public File setSampleIds(List<Long> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public long getJobId() {
        return jobId;
    }

    public File setJobId(long jobId) {
        this.jobId = jobId;
        return this;
    }

    public List<RelatedFile> getRelatedFiles() {
        return relatedFiles;
    }

    public File setRelatedFiles(List<RelatedFile> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public File setAcl(List<FileAclEntry> acl) {
        this.acl = acl;
        return this;
    }

    public FileIndex getIndex() {
        return index;
    }

    public File setIndex(FileIndex index) {
        this.index = index;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public File setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public File setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
