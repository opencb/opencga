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

import java.util.Map;

/**
 * Created by imedina on 11/09/14.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileExperiment {

    @DataField(description = ParamConstants.FILE_EXPERIMENT_TECHNOLOGY_DESCRIPTION)
    private Technology technology;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_METHOD_DESCRIPTION)
    private Method method;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_NUCLEIC_ACID_TYPE_DESCRIPTION)
    private NucleicAcidType nucleicAcidType;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_MANUFACTURER_DESCRIPTION)
    private String manufacturer;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_PLATFORM_DESCRIPTION)
    private String platform;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_LIBRARY_DESCRIPTION)
    private String library;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_DATE_DESCRIPTION)
    private String date;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_CENTER_DESCRIPTION)
    private String center;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_LAB_DESCRIPTION)
    private String lab;
    @DataField(description = ParamConstants.FILE_EXPERIMENT_RESPONSIBLE_DESCRIPTION)
    private String responsible;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public enum Technology {
        SEQUENCING,
        MICROARRAY
    }

    public enum Method {
        WHOLE_EXOME_SEQUENCING,
        WHOLE_GENOME_SEQUENCING,
        TARGETED_DNA_SEQUENCING,
        AMPLICON_SEQUENCING,
        GENOTYPING_MICROARRAY,

    }

    public enum NucleicAcidType {
        DNA,
        RNA
    }

    public FileExperiment() {
    }

    public FileExperiment(Technology technology, Method method, NucleicAcidType nucleicAcidType, String manufacturer, String platform,
                          String library, String date, String center, String lab, String responsible, String description,
                          Map<String, Object> attributes) {
        this.technology = technology;
        this.method = method;
        this.nucleicAcidType = nucleicAcidType;
        this.manufacturer = manufacturer;
        this.platform = platform;
        this.library = library;
        this.date = date;
        this.center = center;
        this.lab = lab;
        this.responsible = responsible;
        this.description = description;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Experiment{");
        sb.append("technology=").append(technology);
        sb.append(", method=").append(method);
        sb.append(", nucleicAcidType=").append(nucleicAcidType);
        sb.append(", manufacturer='").append(manufacturer).append('\'');
        sb.append(", platform='").append(platform).append('\'');
        sb.append(", library='").append(library).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", center='").append(center).append('\'');
        sb.append(", lab='").append(lab).append('\'');
        sb.append(", responsible='").append(responsible).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Technology getTechnology() {
        return technology;
    }

    public FileExperiment setTechnology(Technology technology) {
        this.technology = technology;
        return this;
    }

    public Method getMethod() {
        return method;
    }

    public FileExperiment setMethod(Method method) {
        this.method = method;
        return this;
    }

    public NucleicAcidType getNucleicAcidType() {
        return nucleicAcidType;
    }

    public FileExperiment setNucleicAcidType(NucleicAcidType nucleicAcidType) {
        this.nucleicAcidType = nucleicAcidType;
        return this;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public FileExperiment setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
        return this;
    }

    public String getPlatform() {
        return platform;
    }

    public FileExperiment setPlatform(String platform) {
        this.platform = platform;
        return this;
    }

    public String getLibrary() {
        return library;
    }

    public FileExperiment setLibrary(String library) {
        this.library = library;
        return this;
    }

    public String getDate() {
        return date;
    }

    public FileExperiment setDate(String date) {
        this.date = date;
        return this;
    }

    public String getCenter() {
        return center;
    }

    public FileExperiment setCenter(String center) {
        this.center = center;
        return this;
    }

    public String getLab() {
        return lab;
    }

    public FileExperiment setLab(String lab) {
        this.lab = lab;
        return this;
    }

    public String getResponsible() {
        return responsible;
    }

    public FileExperiment setResponsible(String responsible) {
        this.responsible = responsible;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FileExperiment setDescription(String description) {
        this.description = description;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public FileExperiment setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
