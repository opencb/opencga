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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular
 * analysis. This is the main data model, it stores the most basic and important information.
 */
public class Sample extends Annotable {

    /**
     * Sample ID in the study, this must be unique in the study but can be repeated in different studies. This is a mandatory parameter
     * when creating a new sample, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */
    private String id;

    /**
     * Global unique ID at the whole OpenCGA installation. This is automatically created during the sample creation and cannot be changed.
     *
     * @apiNote Internal, Unique, Immutable
     */
    private String uuid;

    /**
     * Describes how the sample was processed in the lab.
     */
    private SampleProcessing processing;

    /**
     * Describes how the sample was collected.
     *
     * @apiNote
     * @implNote The sample collection is a list of samples
     * @since 2.1
     */
    private SampleCollection collection;

    /**
     * Contains different metrics to evaluate the quality of the sample.
     *
     * @apiNote
     * @implNote The sample collection is a list of samples
     * @see [ZetaGenomics] (https://www.zettagenomics.com)
     * @since 2.1
     */
    private SampleQualityControl qualityControl;

    /**
     * An integer describing the current data release.
     *
     * @apiNote Internal
     */
    private int release;

    /**
     * An integer describing the current version.
     *
     * @apiNote Internal
     */
    private int version;

    /**
     * String representing when the sample was created, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    private String creationDate;

    /**
     * String representing when was the last time the sample was modified, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    private String modificationDate;

    /**
     * An string to describe the properties of the sample.
     *
     * @apiNote
     */
    private String description;

    /**
     * Indicates if the sample is somatic or germline (default)
     *
     * @apiNote
     */
    private boolean somatic;

    /**
     * A List with related phenotypes.
     *
     * @apiNote
     */
    private List<Phenotype> phenotypes;

    /**
     * A reference to the Individual containing this sample. Notice that samples can exist without and Individual ID, this field is not
     * mandatory..
     *
     * @apiNote
     * @see [ZetaGenomics] (https://www.zettagenomics.com)
     */
    private String individualId;

    /**
     * List of File ID containing this sample, eg BAM, VCF, QC images, ...
     *
     * @apiNote
     * @deprecated
     */
    private List<String> fileIds;

    /**
     * An object describing the status of the Sample.
     *
     * @apiNote
     */
    private CustomStatus status;

    /**
     * An object describing the internal information of the Sample. This is managed by OpenCGA.
     *
     * @apiNote Internal
     */
    private SampleInternal internal;

    /**
     * You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.
     *
     * @apiNote
     */
    private Map<String, Object> attributes;

    public Sample() {
    }

    public Sample(String id, String individualId, String description, int release) {
        this(id, null, new SampleProcessing("", "", "", "", "", "", new HashMap<>()), new SampleCollection("", "", "", "", "",
                        new HashMap<>()), release, 1, "", "", description, false, new LinkedList<>(), individualId, new LinkedList<>(),
                new CustomStatus(), null, new LinkedList<>(), new HashMap<>());
    }

    public Sample(String id, String individualId, SampleProcessing processing, SampleCollection collection, int release, int version,
                  String description, boolean somatic, List<Phenotype> phenotypes, List<AnnotationSet> annotationSets, CustomStatus status,
                  SampleInternal internal, Map<String, Object> attributes) {
        this(id, null, processing, collection, release, version, "", "", description, somatic, phenotypes, individualId, new LinkedList<>(),
                status, internal, annotationSets, attributes);
    }

    public Sample(String id, String uuid, SampleProcessing processing, SampleCollection collection, int release, int version,
                  String creationDate, String modificationDate, String description, boolean somatic, List<Phenotype> phenotypes,
                  String individualId, List<String> fileIds, CustomStatus status, SampleInternal internal,
                  List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.id = id;
        this.uuid = uuid;
        this.processing = processing;
        this.collection = collection;
        this.release = release;
        this.version = version;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.description = description;
        this.somatic = somatic;
        this.phenotypes = phenotypes;
        this.individualId = individualId;
        this.fileIds = fileIds;
        this.status = status;
        this.internal = internal;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
    }

    public Sample(String id, String uuid, SampleProcessing processing, SampleCollection collection, SampleQualityControl qualityControl,
                  int release, int version, String creationDate, String modificationDate, String description, boolean somatic,
                  List<Phenotype> phenotypes, String individualId, List<String> fileIds, CustomStatus status, SampleInternal internal,
                  List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.id = id;
        this.uuid = uuid;
        this.processing = processing;
        this.collection = collection;
        this.qualityControl = qualityControl;
        this.release = release;
        this.version = version;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.description = description;
        this.somatic = somatic;
        this.phenotypes = phenotypes;
        this.individualId = individualId;
        this.fileIds = fileIds;
        this.status = status;
        this.internal = internal;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Sample{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", processing=").append(processing);
        sb.append(", collection=").append(collection);
        sb.append(", qualityControl=").append(qualityControl);
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", fileIds=").append(fileIds);
        sb.append(", status=").append(status);
        sb.append(", internal=").append(internal);
        sb.append(", attributes=").append(attributes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

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
                .append(status, sample.status)
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
                .append(status)
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

    @Override
    public String getUuid() {
        return uuid;
    }

    public Sample setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
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

    public SampleQualityControl getQualityControl() {
        return qualityControl;
    }

    public Sample setQualityControl(SampleQualityControl qualityControl) {
        this.qualityControl = qualityControl;
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

    public CustomStatus getStatus() {
        return status;
    }

    public Sample setStatus(CustomStatus status) {
        this.status = status;
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
