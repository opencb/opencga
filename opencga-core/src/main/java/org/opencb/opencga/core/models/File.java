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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opencb.biodata.models.commons.Software;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.acls.AclParams;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class File extends Annotable {

    private String id;
    private String name;
    private String uuid;

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

    private String checksum;

    private URI uri;
    private String path;

    private int release;
    private String creationDate;
    private String modificationDate;

    private String description;
    private FileStatus status;
    private boolean external;

    private long size;
    private Software software;
    private Experiment experiment;
    private List<Sample> samples;

    private List<String> tags;

    /**
     * This field values -1 when file has been uploaded.
     */
    @Deprecated
    private Job job;
    private List<RelatedFile> relatedFiles;

    private FileIndex index;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;


    public File() {
    }

    public File(String name, Type type, Format format, Bioformat bioformat, String path, URI uri, String description, FileStatus status,
                long size, int release) {
        this(name, type, format, bioformat, uri, path, null, TimeUtils.getTime(), TimeUtils.getTime(), description, status,
                false, size, null, new Experiment(), Collections.emptyList(), new Job(), Collections.emptyList(),
                new FileIndex(), release, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
    }

    public File(Type type, Format format, Bioformat bioformat, String path, String description, FileStatus status, long size,
                List<Sample> samples, long jobId, Software software, Map<String, Object> stats, Map<String, Object> attributes) {
        this("", type, format, bioformat, null, path, null, TimeUtils.getTime(), TimeUtils.getTime(), description, status,
                false, size, software, new Experiment(), samples, new Job().setUid(jobId), Collections.emptyList(), new FileIndex(), -1,
                Collections.emptyList(), stats, attributes);
    }

    public File(String name, Type type, Format format, Bioformat bioformat, URI uri, String path, String checksum,
                String creationDate, String modificationDate, String description, FileStatus status, boolean external, long size,
                Software software, Experiment experiment, List<Sample> samples, Job job, List<RelatedFile> relatedFiles, FileIndex index,
                int release, List<AnnotationSet> annotationSets, Map<String, Object> stats, Map<String, Object> attributes) {
        this.id = StringUtils.isNotEmpty(path) ? StringUtils.replace(path, "/", ":") : path;
        this.name = name;
        this.type = type;
        this.format = format;
        this.bioformat = bioformat;
        this.uri = uri;
        this.path = path;
        this.checksum = checksum;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.description = description;
        this.status = status;
        this.release = release;
        this.external = external;
        this.size = size;
        this.software = software;
        this.experiment = experiment;
        this.samples = samples;
        this.tags = Collections.emptyList();
        this.job = job;
        this.relatedFiles = relatedFiles;
        this.index = index != null ? index : new FileIndex();
        this.annotationSets = annotationSets;
        this.stats = stats;
        this.attributes = attributes;
    }

    public static class FileStatus extends Status {

        /**
         * TRASHED name means that the object is marked as deleted although is still available in the database.
         */
        public static final String TRASHED = "TRASHED";

        public static final String STAGE = "STAGE";
        public static final String MISSING = "MISSING";
        public static final String PENDING_DELETE = "PENDING_DELETE";
        public static final String DELETING = "DELETING"; // This status is set exactly before deleting the file from disk.
        public static final String REMOVED = "REMOVED";

        public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, TRASHED, STAGE, MISSING, PENDING_DELETE, DELETING,
                REMOVED);

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
            if (status != null && (status.equals(STAGE) || status.equals(MISSING) || status.equals(TRASHED)
                    || status.equals(PENDING_DELETE) || status.equals(DELETING) || status.equals(REMOVED))) {
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
        BIGWIG,

        SAM,
        BAM,
        BAI,
        CRAM,
        CRAI,
        FASTQ,
        FASTA,
        PED,

        TAB_SEPARATED_VALUES, COMMA_SEPARATED_VALUES, XML, PROTOCOL_BUFFER, JSON, AVRO, PARQUET, //Serialization formats

        IMAGE,
        PLAIN,
        BINARY,
        EXECUTABLE,
        @Deprecated GZIP,
        NONE,
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
        COVERAGE,
        SEQUENCE,
        PEDIGREE,
        REFERENCE_GENOME,
        NONE,
        UNKNOWN
    }

    public static class RelatedFile {

        private File file;
        private Relation relation;

        public enum Relation {
            PRODUCED_FROM,
            PART_OF_PAIR,
            PEDIGREE,
            REFERENCE_GENOME
        }

        public RelatedFile() {
        }

        public RelatedFile(File file, Relation relation) {
            this.file = file;
            this.relation = relation;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RelatedFile{");
            sb.append("file=").append(file);
            sb.append(", relation=").append(relation);
            sb.append('}');
            return sb.toString();
        }

        public File getFile() {
            return file;
        }

        public void setFile(File file) {
            this.file = file;
        }

        public Relation getRelation() {
            return relation;
        }

        public RelatedFile setRelation(Relation relation) {
            this.relation = relation;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            RelatedFile that = (RelatedFile) o;

            return new EqualsBuilder()
                    .append(file.getId(), that.file.getId())
                    .append(relation, that.relation)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(file.getId())
                    .append(relation)
                    .toHashCode();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("File{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", type=").append(type);
        sb.append(", format=").append(format);
        sb.append(", bioformat=").append(bioformat);
        sb.append(", uri=").append(uri);
        sb.append(", path='").append(path).append('\'');
        sb.append(", release=").append(release);
        sb.append(", checksum=").append(checksum);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", status=").append(status);
        sb.append(", external=").append(external);
        sb.append(", size=").append(size);
        sb.append(", software=").append(software);
        sb.append(", experiment=").append(experiment);
        sb.append(", samples=").append(samples);
        sb.append(", tags=").append(tags);
        sb.append(", job=").append(job);
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append(", index=").append(index);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", stats=").append(stats);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public File setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public File setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public File setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public File setId(String id) {
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
        this.id = StringUtils.isNotEmpty(this.path) ? StringUtils.replace(this.path, "/", ":") : this.path;
        return this;
    }

    public String getChecksum() {
        return checksum;
    }

    public File setChecksum(String checksum) {
        this.checksum = checksum;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public File setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public File setRelease(int release) {
        this.release = release;
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

    public long getSize() {
        return size;
    }

    public File setSize(long size) {
        this.size = size;
        return this;
    }

    public List<Sample> getSamples() {
        return samples;
    }

    public File setSamples(List<Sample> samples) {
        this.samples = samples;
        return this;
    }

    public Software getSoftware() {
        return software;
    }

    public File setSoftware(Software software) {
        this.software = software;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public File setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public Experiment getExperiment() {
        return experiment;
    }

    public File setExperiment(Experiment experiment) {
        this.experiment = experiment;
        return this;
    }

    public Job getJob() {
        return job;
    }

    public File setJob(Job job) {
        this.job = job;
        return this;
    }

    public List<RelatedFile> getRelatedFiles() {
        return relatedFiles;
    }

    public File setRelatedFiles(List<RelatedFile> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public FileIndex getIndex() {
        return index;
    }

    public File setIndex(FileIndex index) {
        this.index = index;
        return this;
    }

    public File setAnnotationSets(List<AnnotationSet> annotationSets) {
        super.setAnnotationSets(annotationSets);
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

    // Acl params to communicate the WS and the sample manager
    public static class FileAclParams extends AclParams {

        private String sample;

        public FileAclParams() {
        }

        public FileAclParams(String permissions, Action action, String sample) {
            super(permissions, action);
            this.sample = sample;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FileAclParams{");
            sb.append("permissions='").append(permissions).append('\'');
            sb.append(", action=").append(action);
            sb.append(", sample='").append(sample).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getSample() {
            return sample;
        }

        public FileAclParams setSample(String sample) {
            this.sample = sample;
            return this;
        }
    }

}
