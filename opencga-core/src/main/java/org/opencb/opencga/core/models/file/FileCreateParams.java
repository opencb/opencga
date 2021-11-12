package org.opencb.opencga.core.models.file;

import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.opencga.core.models.common.CustomStatusParams;

import java.util.List;
import java.util.Map;

public class FileCreateParams {

    private String content;
    private String path;
    private String description;
    private File.Type type;
    private File.Format format;
    private File.Bioformat bioformat;
    private List<String> sampleIds;
    private Software software;
    private List<String> tags;
    private String jobId;
    private String creationDate;
    private String modificationDate;
    private CustomStatusParams status;
    private Map<String, Object> attributes;

    public FileCreateParams() {
    }

    public FileCreateParams(String content, String path, String description, File.Type type, File.Format format, File.Bioformat bioformat,
                            List<String> sampleIds, Software software, List<String> tags, String jobId, String creationDate,
                            String modificationDate, CustomStatusParams status, Map<String, Object> attributes) {
        this.content = content;
        this.path = path;
        this.description = description;
        this.type = type;
        this.format = format;
        this.bioformat = bioformat;
        this.sampleIds = sampleIds;
        this.software = software;
        this.tags = tags;
        this.jobId = jobId;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.status = status;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileCreateParams{");
        sb.append("content='").append(content).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", format=").append(format);
        sb.append(", bioformat=").append(bioformat);
        sb.append(", sampleIds=").append(sampleIds);
        sb.append(", software=").append(software);
        sb.append(", tags=").append(tags);
        sb.append(", jobId='").append(jobId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getContent() {
        return content;
    }

    public FileCreateParams setContent(String content) {
        this.content = content;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FileCreateParams setPath(String path) {
        this.path = path;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FileCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public File.Type getType() {
        return type;
    }

    public FileCreateParams setType(File.Type type) {
        this.type = type;
        return this;
    }

    public File.Format getFormat() {
        return format;
    }

    public FileCreateParams setFormat(File.Format format) {
        this.format = format;
        return this;
    }

    public File.Bioformat getBioformat() {
        return bioformat;
    }

    public FileCreateParams setBioformat(File.Bioformat bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public FileCreateParams setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public Software getSoftware() {
        return software;
    }

    public FileCreateParams setSoftware(Software software) {
        this.software = software;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public FileCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public String getJobId() {
        return jobId;
    }

    public FileCreateParams setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public FileCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public FileCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public FileCreateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public FileCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
