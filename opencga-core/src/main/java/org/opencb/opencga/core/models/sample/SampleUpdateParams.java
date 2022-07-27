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

package org.opencb.opencga.core.models.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.ExternalSource;
import org.opencb.opencga.core.models.common.StatusParams;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class SampleUpdateParams {

    private String id;
    private String description;
    private String creationDate;
    private String modificationDate;
    private String individualId;
    private ExternalSource source;
    private SampleProcessing processing;
    private SampleCollection collection;
    private SampleQualityControl qualityControl;
    private Boolean somatic;
    private List<Phenotype> phenotypes;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;
    private StatusParams status;

    public SampleUpdateParams() {
    }

    public SampleUpdateParams(String id, String description, String creationDate, String modificationDate, String individualId,
                              ExternalSource source, SampleProcessing processing, SampleCollection collection,
                              SampleQualityControl qualityControl, Boolean somatic, List<Phenotype> phenotypes,
                              List<AnnotationSet> annotationSets, Map<String, Object> attributes, StatusParams status) {
        this.id = id;
        this.description = description;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.individualId = individualId;
        this.source = source;
        this.processing = processing;
        this.collection = collection;
        this.qualityControl = qualityControl;
        this.somatic = somatic;
        this.phenotypes = phenotypes;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
        this.status = status;
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

    @JsonIgnore
    public Sample toSample() {
        return new Sample(id, "", source, processing, collection, qualityControl, 1, 1, creationDate, modificationDate,
                description, somatic != null && somatic, phenotypes, individualId, Collections.emptyList(), Collections.emptyList(),
                status != null ? status.toStatus() : null, new SampleInternal(), annotationSets, attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", source=").append(source);
        sb.append(", processing=").append(processing);
        sb.append(", collection=").append(collection);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }


    public String getId() {
        return id;
    }

    public SampleUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SampleUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public SampleUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public SampleUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public SampleUpdateParams setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public ExternalSource getSource() {
        return source;
    }

    public SampleUpdateParams setSource(ExternalSource source) {
        this.source = source;
        return this;
    }

    public SampleProcessing getProcessing() {
        return processing;
    }

    public SampleUpdateParams setProcessing(SampleProcessing processing) {
        this.processing = processing;
        return this;
    }

    public SampleCollection getCollection() {
        return collection;
    }

    public SampleUpdateParams setCollection(SampleCollection collection) {
        this.collection = collection;
        return this;
    }

    public SampleQualityControl getQualityControl() {
        return qualityControl;
    }

    public SampleUpdateParams setQualityControl(SampleQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public Boolean getSomatic() {
        return somatic;
    }

    public SampleUpdateParams setSomatic(Boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public SampleUpdateParams setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public SampleUpdateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public SampleUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public StatusParams getStatus() {
        return status;
    }

    public SampleUpdateParams setStatus(StatusParams status) {
        this.status = status;
        return this;
    }
}
