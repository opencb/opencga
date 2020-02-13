package org.opencb.opencga.core.models.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class FileUpdateParams {

    private String name;
    private String description;

    private List<String> samples;

    private String checksum;
    private File.Format format;
    private File.Bioformat bioformat;
    private Software software;
    private FileExperiment experiment;
    private List<String> tags;
    private File.FileStatus status;

    private List<SmallRelatedFileParams> relatedFiles;

    private List<AnnotationSet> annotationSets;
    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    public FileUpdateParams() {
    }

    public FileUpdateParams(String name, String description, List<String> samples, String checksum, File.Format format,
                            File.Bioformat bioformat, Software software, FileExperiment experiment, List<String> tags, File.FileStatus status,
                            List<SmallRelatedFileParams> relatedFiles, List<AnnotationSet> annotationSets, Map<String, Object> stats,
                            Map<String, Object> attributes) {
        this.name = name;
        this.description = description;
        this.samples = samples;
        this.checksum = checksum;
        this.format = format;
        this.bioformat = bioformat;
        this.software = software;
        this.experiment = experiment;
        this.tags = tags;
        this.status = status;
        this.relatedFiles = relatedFiles;
        this.annotationSets = annotationSets;
        this.stats = stats;
        this.attributes = attributes;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        List<AnnotationSet> annotationSetList = this.annotationSets;
        this.annotationSets = null;

        ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));

        this.annotationSets = annotationSetList;
        if (this.annotationSets != null) {
            // We leave annotation sets as is so we don't need to make any more castings
            params.put("annotationSets", this.annotationSets);
        }

        return params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", checksum='").append(checksum).append('\'');
        sb.append(", format=").append(format);
        sb.append(", bioformat=").append(bioformat);
        sb.append(", software=").append(software);
        sb.append(", experiment=").append(experiment);
        sb.append(", tags=").append(tags);
        sb.append(", status=").append(status);
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", stats=").append(stats);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public FileUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FileUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public FileUpdateParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public String getChecksum() {
        return checksum;
    }

    public FileUpdateParams setChecksum(String checksum) {
        this.checksum = checksum;
        return this;
    }

    public File.Format getFormat() {
        return format;
    }

    public FileUpdateParams setFormat(File.Format format) {
        this.format = format;
        return this;
    }

    public File.Bioformat getBioformat() {
        return bioformat;
    }

    public FileUpdateParams setBioformat(File.Bioformat bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public File.FileStatus getStatus() {
        return status;
    }

    public FileUpdateParams setStatus(File.FileStatus status) {
        this.status = status;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public FileUpdateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public Software getSoftware() {
        return software;
    }

    public FileUpdateParams setSoftware(Software software) {
        this.software = software;
        return this;
    }

    public FileExperiment getExperiment() {
        return experiment;
    }

    public FileUpdateParams setExperiment(FileExperiment experiment) {
        this.experiment = experiment;
        return this;
    }

    public List<SmallRelatedFileParams> getRelatedFiles() {
        return relatedFiles;
    }

    public FileUpdateParams setRelatedFiles(List<SmallRelatedFileParams> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public FileUpdateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public FileUpdateParams setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public FileUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
