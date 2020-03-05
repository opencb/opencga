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

package org.opencb.opencga.core.models.file;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Software;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.sample.Sample;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    private boolean external;

    private long size;
    private Software software;
    private FileExperiment experiment;
    private List<Sample> samples;
    private String jobId;
    private List<String> tags;
    private List<FileRelatedFile> relatedFiles;

    private Map<String, Object> stats;
    private FileInternal internal;
    private Map<String, Object> attributes;

    public File() {
    }

    public File(String name, Type type, Format format, Bioformat bioformat, String path, URI uri, String description, FileInternal internal,
                long size, int release) {
        this(name, type, format, bioformat, uri, path, null, TimeUtils.getTime(), TimeUtils.getTime(), description,
                false, size, new Software(), new FileExperiment(), Collections.emptyList(), Collections.emptyList(), "", release,
                Collections.emptyList(), Collections.emptyMap(), internal, Collections.emptyMap());
    }

    public File(Type type, Format format, Bioformat bioformat, String path, String description, FileInternal internal, long size,
                List<Sample> samples, Software software, Map<String, Object> stats, Map<String, Object> attributes) {
        this("", type, format, bioformat, null, path, null, TimeUtils.getTime(), TimeUtils.getTime(), description,
                false, size, software, new FileExperiment(), samples, Collections.emptyList(), "", -1, Collections.emptyList(), stats, internal,
                attributes);
    }

    public File(String name, Type type, Format format, Bioformat bioformat, URI uri, String path, String checksum, String creationDate,
                String modificationDate, String description, boolean external, long size, Software software, FileExperiment experiment,
                List<Sample> samples, List<FileRelatedFile> relatedFiles, String jobId, int release, List<AnnotationSet> annotationSets,
                Map<String, Object> stats, FileInternal internal, Map<String, Object> attributes) {
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
        this.release = release;
        this.external = external;
        this.internal = internal;
        this.size = size;
        this.software = software;
        this.experiment = experiment;
        this.samples = samples;
        this.tags = Collections.emptyList();
        this.relatedFiles = relatedFiles;
        this.annotationSets = annotationSets;
        this.jobId = jobId;
        this.stats = stats;
        this.attributes = attributes;
    }

    public enum Type {
        FILE,
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
        sb.append(", internal=").append(internal);
        sb.append(", size=").append(size);
        sb.append(", software=").append(software);
        sb.append(", experiment=").append(experiment);
        sb.append(", samples=").append(samples);
        sb.append(", jobId='").append(jobId).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", relatedFiles=").append(relatedFiles);
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

    public boolean isExternal() {
        return external;
    }

    public File setExternal(boolean external) {
        this.external = external;
        return this;
    }

    public FileInternal getInternal() {
        return internal;
    }

    public File setInternal(FileInternal internal) {
        this.internal = internal;
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

    public FileExperiment getExperiment() {
        return experiment;
    }

    public File setExperiment(FileExperiment experiment) {
        this.experiment = experiment;
        return this;
    }

    public List<FileRelatedFile> getRelatedFiles() {
        return relatedFiles;
    }

    public File setRelatedFiles(List<FileRelatedFile> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public String getJobId() {
        return jobId;
    }

    public File setJobId(String jobId) {
        this.jobId = jobId;
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
}
