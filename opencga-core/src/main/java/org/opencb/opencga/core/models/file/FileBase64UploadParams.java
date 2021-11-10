package org.opencb.opencga.core.models.file;

import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.opencga.core.models.common.CustomStatusParams;

import java.util.List;

public class FileBase64UploadParams {

    private String base64;
    private String path;
    private String description;
    private File.Bioformat bioformat;
    private List<String> sampleIds;
    private Software software;
    private List<String> tags;
    private String creationDate;
    private String modificationDate;
    private CustomStatusParams status;

    public FileBase64UploadParams() {
    }

    public FileBase64UploadParams(String base64, String path, String description, File.Bioformat bioformat, List<String> sampleIds,
                                  Software software, List<String> tags, String creationDate, String modificationDate,
                                  CustomStatusParams status) {
        this.base64 = base64;
        this.path = path;
        this.description = description;
        this.bioformat = bioformat;
        this.sampleIds = sampleIds;
        this.software = software;
        this.tags = tags;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileBase64UploadParams{");
        sb.append("base64='").append(base64).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", bioformat=").append(bioformat);
        sb.append(", sampleIds=").append(sampleIds);
        sb.append(", software=").append(software);
        sb.append(", tags=").append(tags);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getBase64() {
        return base64;
    }

    public FileBase64UploadParams setBase64(String base64) {
        this.base64 = base64;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FileBase64UploadParams setPath(String path) {
        this.path = path;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FileBase64UploadParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public File.Bioformat getBioformat() {
        return bioformat;
    }

    public FileBase64UploadParams setBioformat(File.Bioformat bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public FileBase64UploadParams setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
        return this;
    }

    public Software getSoftware() {
        return software;
    }

    public FileBase64UploadParams setSoftware(Software software) {
        this.software = software;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public FileBase64UploadParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public FileBase64UploadParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public FileBase64UploadParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public FileBase64UploadParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }
}
