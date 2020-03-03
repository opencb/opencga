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

package org.opencb.opencga.core.models.sample;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Sample extends Annotable {

    private String id;
    private String uuid;
    private SampleProcessing processing;
    private SampleCollection collection;

    private int release;
    private int version;
    private String creationDate;
    private String modificationDate;
    private String description;
    private boolean somatic;
    private List<Phenotype> phenotypes;

    private String individualId;
    private List<String> fileIds;

    private SampleInternal internal;

    private Map<String, Object> attributes;


    public Sample() {
    }

    public Sample(String id, String individualId, String description, int release) {
        this(id, individualId, null, null, release, 1, description, false, new ArrayList<>(), new ArrayList<>(), null, new HashMap<>());
    }

    public Sample(String id, String individualId, SampleProcessing processing, SampleCollection collection, int release, int version,
                  String description, boolean somatic, List<Phenotype> phenotypes, List<AnnotationSet> annotationSets,
                  SampleInternal internal, Map<String, Object> attributes) {
        this.id = id;
        this.processing = processing;
        this.collection = collection;
        this.release = release;
        this.version = version;
        this.creationDate = TimeUtils.getTime();
        this.description = description;
        this.somatic = somatic;
        this.phenotypes = phenotypes;
        this.annotationSets = annotationSets;
        this.individualId = individualId;
        this.internal = internal;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Sample{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", processing=").append(processing);
        sb.append(", collection=").append(collection);
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", internal=").append(internal);
        sb.append(", attributes=").append(attributes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Sample sample = (Sample) o;

        return new EqualsBuilder()
                .append(release, sample.release)
                .append(version, sample.version)
                .append(somatic, sample.somatic)
                .append(id, sample.id)
                .append(uuid, sample.uuid)
                .append(processing, sample.processing)
                .append(collection, sample.collection)
                .append(individualId, sample.individualId)
                .append(creationDate, sample.creationDate)
                .append(modificationDate, sample.modificationDate)
                .append(description, sample.description)
                .append(phenotypes, sample.phenotypes)
                .append(internal, sample.internal)
                .append(attributes, sample.attributes)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(uuid)
                .append(processing)
                .append(collection)
                .append(individualId)
                .append(release)
                .append(version)
                .append(creationDate)
                .append(modificationDate)
                .append(description)
                .append(somatic)
                .append(phenotypes)
                .append(internal)
                .append(attributes)
                .toHashCode();
    }

    @Override
    public Sample setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public Sample setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Sample setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Sample setId(String id) {
        this.id = id;
        return this;
    }

    public SampleProcessing getProcessing() {
        return processing;
    }

    public Sample setProcessing(SampleProcessing processing) {
        this.processing = processing;
        return this;
    }

    public SampleCollection getCollection() {
        return collection;
    }

    public Sample setCollection(SampleCollection collection) {
        this.collection = collection;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public Sample setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Sample setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Sample setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Sample setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isSomatic() {
        return somatic;
    }

    public Sample setSomatic(boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Sample setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Sample setVersion(int version) {
        this.version = version;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public Sample setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getFileIds() {
        return fileIds;
    }

    public Sample setFileIds(List<String> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public SampleInternal getInternal() {
        return internal;
    }

    public Sample setInternal(SampleInternal internal) {
        this.internal = internal;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Sample setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    @Override
    public Sample setAnnotationSets(List<AnnotationSet> annotationSets) {
        super.setAnnotationSets(annotationSets);
        return this;
    }

}
