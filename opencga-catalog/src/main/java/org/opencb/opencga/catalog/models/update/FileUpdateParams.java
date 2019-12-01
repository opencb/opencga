package org.opencb.opencga.catalog.models.update;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.File;

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
    private File.FileStatus status;

    private List<AnnotationSet> annotationSets;
    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    public FileUpdateParams() {
    }

    public ObjectMap getUpdateMap() throws CatalogException {
        try {
            List<AnnotationSet> annotationSetList = this.annotationSets;
            this.annotationSets = null;

            ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));

            this.annotationSets = annotationSetList;
            if (this.annotationSets != null) {
                // We leave annotation sets as is so we don't need to make any more castings
                params.put("annotationSets", this.annotationSets);
            }

            return params;
        } catch (JsonProcessingException e) {
            throw new CatalogException(e);
        }
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
        sb.append(", status=").append(status);
        sb.append(", software=").append(software);
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

    public Software getSoftware() {
        return software;
    }

    public FileUpdateParams setSoftware(Software software) {
        this.software = software;
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
