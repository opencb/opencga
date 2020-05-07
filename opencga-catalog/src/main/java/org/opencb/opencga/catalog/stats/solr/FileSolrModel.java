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

package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class FileSolrModel extends CatalogSolrModel {

    @Field
    private String name;

    @Field
    private String type;

    @Field
    private String format;

    @Field
    private String bioformat;

    @Field
    private boolean external;

    @Field
    private long size;

    @Field
    private String softwareName;

    @Field
    private String softwareVersion;

    @Field
    private String experimentTechnology;

    @Field
    private String experimentMethod;

    @Field
    private String experimentNucleicAcidType;

    @Field
    private String experimentManufacturer;

    @Field
    private String experimentPlatform;

    @Field
    private String experimentLibrary;

    @Field
    private String experimentCenter;

    @Field
    private String experimentLab;

    @Field
    private String experimentResponsible;

    @Field
    private List<String> tags;

    @Field
    private int numSamples;

    @Field
    private int numRelatedFiles;

    @Field
    private List<String> annotationSets;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public FileSolrModel() {
        this.annotationSets = new ArrayList<>();
        this.annotations = new HashMap<>();
        this.tags = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileSolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", format='").append(format).append('\'');
        sb.append(", bioformat='").append(bioformat).append('\'');
        sb.append(", release=").append(release);
        sb.append(", creationYear=").append(creationYear);
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay=").append(creationDay);
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", external=").append(external);
        sb.append(", size=").append(size);
        sb.append(", softwareName='").append(softwareName).append('\'');
        sb.append(", softwareVersion='").append(softwareVersion).append('\'');
        sb.append(", experimentTechnology='").append(experimentTechnology).append('\'');
        sb.append(", experimentMethod='").append(experimentMethod).append('\'');
        sb.append(", experimentNucleicAcidType='").append(experimentNucleicAcidType).append('\'');
        sb.append(", experimentManufacturer='").append(experimentManufacturer).append('\'');
        sb.append(", experimentPlatform='").append(experimentPlatform).append('\'');
        sb.append(", experimentLibrary='").append(experimentLibrary).append('\'');
        sb.append(", experimentCenter='").append(experimentCenter).append('\'');
        sb.append(", experimentLab='").append(experimentLab).append('\'');
        sb.append(", experimentResponsible='").append(experimentResponsible).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", numSamples=").append(numSamples);
        sb.append(", numRelatedFiles=").append(numRelatedFiles);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public FileSolrModel setName(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public FileSolrModel setType(String type) {
        this.type = type;
        return this;
    }

    public String getFormat() {
        return format;
    }

    public FileSolrModel setFormat(String format) {
        this.format = format;
        return this;
    }

    public String getBioformat() {
        return bioformat;
    }

    public FileSolrModel setBioformat(String bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public boolean isExternal() {
        return external;
    }

    public FileSolrModel setExternal(boolean external) {
        this.external = external;
        return this;
    }

    public long getSize() {
        return size;
    }

    public FileSolrModel setSize(long size) {
        this.size = size;
        return this;
    }

    public String getSoftwareName() {
        return softwareName;
    }

    public FileSolrModel setSoftwareName(String softwareName) {
        this.softwareName = softwareName;
        return this;
    }

    public String getSoftwareVersion() {
        return softwareVersion;
    }

    public FileSolrModel setSoftwareVersion(String softwareVersion) {
        this.softwareVersion = softwareVersion;
        return this;
    }

    public String getExperimentTechnology() {
        return experimentTechnology;
    }

    public FileSolrModel setExperimentTechnology(String experimentTechnology) {
        this.experimentTechnology = experimentTechnology;
        return this;
    }

    public String getExperimentMethod() {
        return experimentMethod;
    }

    public FileSolrModel setExperimentMethod(String experimentMethod) {
        this.experimentMethod = experimentMethod;
        return this;
    }

    public String getExperimentNucleicAcidType() {
        return experimentNucleicAcidType;
    }

    public FileSolrModel setExperimentNucleicAcidType(String experimentNucleicAcidType) {
        this.experimentNucleicAcidType = experimentNucleicAcidType;
        return this;
    }

    public String getExperimentManufacturer() {
        return experimentManufacturer;
    }

    public FileSolrModel setExperimentManufacturer(String experimentManufacturer) {
        this.experimentManufacturer = experimentManufacturer;
        return this;
    }

    public String getExperimentPlatform() {
        return experimentPlatform;
    }

    public FileSolrModel setExperimentPlatform(String experimentPlatform) {
        this.experimentPlatform = experimentPlatform;
        return this;
    }

    public String getExperimentLibrary() {
        return experimentLibrary;
    }

    public FileSolrModel setExperimentLibrary(String experimentLibrary) {
        this.experimentLibrary = experimentLibrary;
        return this;
    }

    public String getExperimentCenter() {
        return experimentCenter;
    }

    public FileSolrModel setExperimentCenter(String experimentCenter) {
        this.experimentCenter = experimentCenter;
        return this;
    }

    public String getExperimentLab() {
        return experimentLab;
    }

    public FileSolrModel setExperimentLab(String experimentLab) {
        this.experimentLab = experimentLab;
        return this;
    }

    public String getExperimentResponsible() {
        return experimentResponsible;
    }

    public FileSolrModel setExperimentResponsible(String experimentResponsible) {
        this.experimentResponsible = experimentResponsible;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public FileSolrModel setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public FileSolrModel setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    public int getNumRelatedFiles() {
        return numRelatedFiles;
    }

    public FileSolrModel setNumRelatedFiles(int numRelatedFiles) {
        this.numRelatedFiles = numRelatedFiles;
        return this;
    }

    public List<String> getAnnotationSets() {
        return annotationSets;
    }

    public FileSolrModel setAnnotationSets(List<String> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public FileSolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}


