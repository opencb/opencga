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

package org.opencb.opencga.core.models.file;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.common.Status;
import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
@DataClass(id = "File", since = "1.0",
        description = "File data model hosts information about any file.")
public class File extends Annotable {

    /**
     * File ID is a mandatory parameter when creating a new File, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */
    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    /**
     * Global unique ID at the whole OpenCGA installation. This is automatically created during the File creation and cannot be changed.
     *
     * @apiNote Internal, Unique, Immutable
     */
    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "name", indexed = true,
            description = FieldConstants.FILE_NAME)
    private String name;

    @DataField(id = "type", indexed = true,
            description = FieldConstants.FILE_TYPE)
    private Type type;

    @DataField(id = "format", indexed = true,
            description = FieldConstants.FILE_FORMAT)
    private Format format;

    @DataField(id = "bioformat", indexed = true,
            description = FieldConstants.FILE_BIOFORMAT)
    private Bioformat bioformat;

    @DataField(id = "checksum", indexed = true,
            description = FieldConstants.FILE_CHECKSUM)
    private String checksum;


    @DataField(id = "uri", indexed = true,
            description = FieldConstants.FILE_URI)
    private URI uri;

    @DataField(id = "path", indexed = true,
            description = FieldConstants.FILE_PATH)
    private String path;

    /**
     * An integer describing the current data release.
     *
     * @apiNote Internal
     */
    @DataField(id = "release", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;

    /**
     * String representing when the Cohort was created, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "creationDate", indexed = true,
            description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    /**
     * String representing when was the last time the Cohort was modified, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "modificationDate", indexed = true, since = "1.0",
            description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    /**
     * An string to describe the properties of the File.
     *
     * @apiNote
     */
    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "external",
            description = FieldConstants.FILE_EXTERNAL)
    private boolean external;

    @DataField(id = "size",
            description = FieldConstants.FILE_SIZE)
    private long size;

    @DataField(id = "software",
            description = FieldConstants.FILE_SOFTWARE)
    private Software software;

    @DataField(id = "experiment",
            description = FieldConstants.FILE_EXPERIMENT)
    private FileExperiment experiment;

    @DataField(id = "sampleIds",
            description = FieldConstants.FILE_SAMPLE_IDS)
    private List<String> sampleIds;

    @DataField(id = "jobId",
            description = FieldConstants.FILE_JOB_ID)
    private String jobId;

    @DataField(id = "tags",
            description = FieldConstants.FILE_TAGS)
    private List<String> tags;

    @DataField(id = "relatedFiles",
            description = FieldConstants.FILE_RELATED_FILES)
    private List<FileRelatedFile> relatedFiles;

    @DataField(id = "qualityControl",
            description = FieldConstants.GENERIC_QUALITY_CONTROL)
    private FileQualityControl qualityControl;


    @DataField(id = "stats", deprecated = true,
            description = FieldConstants.GENERIC_STATS)
    @Deprecated
    private Map<String, Object> stats;

    /**
     * An object describing the status of the File.
     *
     * @apiNote
     */

    @DataField(id = "status",
            description = FieldConstants.GENERIC_CUSTOM_STATUS)
    private Status status;


    /**
     * An object describing the internal information of the File. This is managed by OpenCGA.
     *
     * @apiNote Internal
     */

    @DataField(id = "internal",
            description = FieldConstants.GENERIC_INTERNAL)
    private FileInternal internal;

    /**
     * You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.
     *
     * @apiNote
     */
    @DataField(id = "attributes",
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public File() {
    }

    public File(String name, Type type, Format format, Bioformat bioformat, String path, URI uri, String description, FileInternal internal,
                long size, int release) {
        this(name, type, format, bioformat, uri, path, null, TimeUtils.getTime(), TimeUtils.getTime(), description,
                false, size, new Software(), new FileExperiment(), Collections.emptyList(), Collections.emptyList(), "", release,
                Collections.emptyList(), Collections.emptyList(), new FileQualityControl(), Collections.emptyMap(), new Status(),
                internal, Collections.emptyMap());
    }

    public File(Type type, Format format, Bioformat bioformat, String path, String description, FileInternal internal, long size,
                List<String> sampleIds, Software software, String jobId, FileQualityControl qualityControl, Map<String, Object> stats,
                Map<String, Object> attributes) {
        this("", type, format, bioformat, null, path, null, TimeUtils.getTime(), TimeUtils.getTime(), description,
                false, size, software, new FileExperiment(), sampleIds, Collections.emptyList(), jobId, -1, Collections.emptyList(),
                Collections.emptyList(), qualityControl, stats, new Status(), internal, attributes);
    }

    public File(String name, Type type, Format format, Bioformat bioformat, URI uri, String path, String checksum, String creationDate,
                String modificationDate, String description, boolean external, long size, Software software, FileExperiment experiment,
                List<String> sampleIds, List<FileRelatedFile> relatedFiles, String jobId, int release, List<String> tags,
                List<AnnotationSet> annotationSets, FileQualityControl qualityControl, Map<String, Object> stats, Status status,
                FileInternal internal, Map<String, Object> attributes) {
        id = StringUtils.isNotEmpty(path) ? StringUtils.replace(path, "/", ":") : path;
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
        this.release = release;
        this.external = external;
        this.internal = internal;
        this.size = size;
        this.software = software;
        this.experiment = experiment;
        this.sampleIds = sampleIds;
        this.tags = tags;
        this.relatedFiles = relatedFiles;
        this.annotationSets = annotationSets;
        this.jobId = jobId;
        this.qualityControl = qualityControl;
        this.stats = stats;
        this.status = status;
        this.attributes = attributes;
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
        sb.append(", checksum='").append(checksum).append('\'');
        sb.append(", uri=").append(uri);
        sb.append(", path='").append(path).append('\'');
        sb.append(", release=").append(release);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", external=").append(external);
        sb.append(", size=").append(size);
        sb.append(", software=").append(software);
        sb.append(", experiment=").append(experiment);
        sb.append(", sampleIds=").append(sampleIds);
        sb.append(", jobId='").append(jobId).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", stats=").append(stats);
        sb.append(", status=").append(status);
        sb.append(", internal=").append(internal);
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

    @Override
    public String getId() {
        return id;
    }

    @Override
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

    @Override
    public String getUuid() {
        return uuid;
    }

    public File setUuid(String uuid) {
        this.uuid = uuid;
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

    public String getChecksum() {
        return checksum;
    }

    public File setChecksum(String checksum) {
        this.checksum = checksum;
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

    public int getRelease() {
        return release;
    }

    public File setRelease(int release) {
        this.release = release;
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

    public Software getSoftware() {
        return software;
    }

    public File setSoftware(Software software) {
        this.software = software;
        return this;
    }

    public FileExperiment getExperiment() {
        return experiment;
    }

    public File setExperiment(FileExperiment experiment) {
        this.experiment = experiment;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public File setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public String getJobId() {
        return jobId;
    }

    public File setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public File setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public List<FileRelatedFile> getRelatedFiles() {
        return relatedFiles;
    }

    public File setRelatedFiles(List<FileRelatedFile> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public FileQualityControl getQualityControl() {
        return qualityControl;
    }

    public File setQualityControl(FileQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public File setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public File setStatus(Status status) {
        this.status = status;
        return this;
    }

    public FileInternal getInternal() {
        return internal;
    }

    public File setInternal(FileInternal internal) {
        this.internal = internal;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public File setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public enum Type {
        FILE,
        VIRTUAL,
        DIRECTORY
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

        PDF,
        IMAGE,
        PLAIN,
        BINARY,
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
}
