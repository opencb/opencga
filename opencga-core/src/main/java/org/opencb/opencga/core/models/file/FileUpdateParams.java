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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.StatusParams;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileUpdateParams {

    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;
    @DataField(description = ParamConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_SAMPLE_IDS_DESCRIPTION)
    private List<String> sampleIds;

    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_CHECKSUM_DESCRIPTION)
    private String checksum;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_FORMAT_DESCRIPTION)
    private File.Format format;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_BIOFORMAT_DESCRIPTION)
    private File.Bioformat bioformat;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_SOFTWARE_DESCRIPTION)
    private Software software;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_EXPERIMENT_DESCRIPTION)
    private FileExperiment experiment;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_TAGS_DESCRIPTION)
    private List<String> tags;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_INTERNAL_DESCRIPTION)
    private SmallFileInternal internal;

    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_RELATED_FILES_DESCRIPTION)
    private List<SmallRelatedFileParams> relatedFiles;

    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_SIZE_DESCRIPTION)
    private Long size;

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private StatusParams status;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_ANNOTATION_SETS_DESCRIPTION)
    private List<AnnotationSet> annotationSets;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_QUALITY_CONTROL_DESCRIPTION)
    private FileQualityControl qualityControl;
    @DataField(description = ParamConstants.FILE_UPDATE_PARAMS_STATS_DESCRIPTION)
    private Map<String, Object> stats;
    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public FileUpdateParams() {
    }

    public FileUpdateParams(String name, String description, String creationDate, String modificationDate, List<String> sampleIds,
                            String checksum, File.Format format, File.Bioformat bioformat, Software software, FileExperiment experiment,
                            List<String> tags, SmallFileInternal internal, Long size, List<SmallRelatedFileParams> relatedFiles,
                            StatusParams status, List<AnnotationSet> annotationSets, FileQualityControl qualityControl,
                            Map<String, Object> stats, Map<String, Object> attributes) {
        this.name = name;
        this.description = description;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.sampleIds = sampleIds;
        this.checksum = checksum;
        this.format = format;
        this.bioformat = bioformat;
        this.software = software;
        this.experiment = experiment;
        this.tags = tags;
        this.internal = internal;
        this.size = size;
        this.relatedFiles = relatedFiles;
        this.status = status;
        this.annotationSets = annotationSets;
        this.qualityControl = qualityControl;
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
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", sampleIds=").append(sampleIds);
        sb.append(", checksum='").append(checksum).append('\'');
        sb.append(", format=").append(format);
        sb.append(", bioformat=").append(bioformat);
        sb.append(", software=").append(software);
        sb.append(", experiment=").append(experiment);
        sb.append(", tags=").append(tags);
        sb.append(", internal=").append(internal);
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append(", size=").append(size);
        sb.append(", status=").append(status);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", qualityControl=").append(qualityControl);
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

    public String getCreationDate() {
        return creationDate;
    }

    public FileUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public FileUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<String> getSampleIds() {
        return sampleIds;
    }

    public FileUpdateParams setSampleIds(List<String> sampleIds) {
        this.sampleIds = sampleIds;
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

    public SmallFileInternal getInternal() {
        return internal;
    }

    public FileUpdateParams setInternal(SmallFileInternal internal) {
        this.internal = internal;
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

    public Long getSize() {
        return size;
    }

    public FileUpdateParams setSize(Long size) {
        this.size = size;
        return this;
    }

    public List<SmallRelatedFileParams> getRelatedFiles() {
        return relatedFiles;
    }

    public FileUpdateParams setRelatedFiles(List<SmallRelatedFileParams> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public StatusParams getStatus() {
        return status;
    }

    public FileUpdateParams setStatus(StatusParams status) {
        this.status = status;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public FileUpdateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public FileQualityControl getQualityControl() {
        return qualityControl;
    }

    public FileUpdateParams setQualityControl(FileQualityControl qualityControl) {
        this.qualityControl = qualityControl;
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
