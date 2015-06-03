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

    private int id;

    /**
     * File name.
     */
    private String name;

    /**
     * Formats: file, folder, index
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

    private Status status;
    private long diskUsage;

    //private int studyId;
    private int experimentId;
    private List<Integer> sampleIds;

    /**
     * This field values -1 when file has been uploaded.
     */
    private int jobId;
    private List<Acl> acl;

    private Index index;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    /* Status */
    public enum Status {
        STAGE,
        READY,
        MISSING,
        TRASHED,
        DELETED
    }

    public enum Type {
        FOLDER,
        FILE
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

    public enum Compression {
        GZIP,
        BGZIP,
        ZIP,
        SNAPPY,
        NONE,
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

    /* Attributes known values */
    public static final String DELETE_DATE = "deleteDate";      //Long

    /**
     * To think:
     * ACL, url,  responsible,  extended source ??
     */

    public File() {
    }

    public File(String name, Type type, Format format, Bioformat bioformat, String path, String ownerId,
                String description, Status status, long diskUsage) {
        this(-1, name, type, format, bioformat, path, ownerId, TimeUtils.getTime(), description, status, diskUsage,
                -1, new LinkedList<Integer>(), -1, new LinkedList<Acl>(), new HashMap<String, Object>(),
                new HashMap<String, Object>());
    }

    public File(String name, Type type, Format format, Bioformat bioformat, String path, String ownerId,
                String creationDate, String description, Status status, long diskUsage) {
        this(-1, name, type, format, bioformat, path, ownerId, creationDate, description, status, diskUsage,
                -1, new LinkedList<Integer>(), -1, new LinkedList<Acl>(), new HashMap<String, Object>(),
                new HashMap<String, Object>());
    }

    public File(int id, String name, Type type, Format format, Bioformat bioformat, String path, String ownerId,
                String creationDate, String description, Status status, long diskUsage, int experimentId,
                List<Integer> sampleIds, int jobId, List<Acl> acl, Map<String, Object> stats,
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
        this.acl = acl;
        this.index = null;
        this.stats = stats;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "File {" + "\n\t" +
                "id:" + id + "\n\t" +
                ", name:'" + name + '\'' + "\n\t" +
                ", type:'" + type + '\'' + "\n\t" +
                ", format:'" + format + '\'' + "\n\t" +
                ", bioformat:'" + bioformat + '\'' + "\n\t" +
//                ", uriScheme:'" + uriScheme + '\'' + "\n\t" +
                ", path:'" + path + '\'' + "\n\t" +
                ", ownerId:'" + ownerId + '\'' + "\n\t" +
                ", creationDate:'" + creationDate + '\'' + "\n\t" +
                ", modificationDate:'" + modificationDate + '\'' + "\n\t" +
                ", description:'" + description + '\'' + "\n\t" +
                ", status:'" + status + '\'' + "\n\t" +
                ", diskUsage:" + diskUsage + "\n\t" +
                ", experimentId:" + experimentId + "\n\t" +
                ", sampleIds:" + sampleIds + "\n\t" +
                ", jobId:" + jobId + "\n\t" +
                ", acl:" + acl + "\n\t" +
                ", stats:" + stats + "\n\t" +
                ", attributes:" + attributes + "\n\t" +
//                ", indices:" + indices + "\n" +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
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

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public int getExperimentId() {
        return experimentId;
    }

    public void setExperimentId(int experimentId) {
        this.experimentId = experimentId;
    }

    public List<Integer> getSampleIds() {
        return sampleIds;
    }

    public void setSampleIds(List<Integer> sampleIds) {
        this.sampleIds = sampleIds;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public List<Acl> getAcl() {
        return acl;
    }

    public void setAcl(List<Acl> acl) {
        this.acl = acl;
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
}
