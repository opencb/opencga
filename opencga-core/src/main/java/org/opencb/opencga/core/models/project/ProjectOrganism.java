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

package org.opencb.opencga.core.models.project;

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ProjectOrganism {

    @DataField(description = ParamConstants.PROJECT_ORGANISM_SCIENTIFIC_NAME_DESCRIPTION)
    private String scientificName;
    @DataField(description = ParamConstants.PROJECT_ORGANISM_COMMON_NAME_DESCRIPTION)
    private String commonName;
    @DataField(description = ParamConstants.PROJECT_ORGANISM_ASSEMBLY_DESCRIPTION)
    private String assembly;

    public ProjectOrganism() {
    }

    public ProjectOrganism(String scientificName, String assembly) {
        this(scientificName, "", assembly);
    }

    public ProjectOrganism(String scientificName, String commonName, String assembly) {
        this.scientificName = scientificName != null ? scientificName : "";
        this.commonName = commonName != null ? commonName : "";
        this.assembly = assembly != null ? assembly : "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectOrganism organism = (ProjectOrganism) o;
        return Objects.equals(scientificName, organism.scientificName)
                && Objects.equals(commonName, organism.commonName)
                && Objects.equals(assembly, organism.assembly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scientificName, commonName, assembly);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Organism{");
        sb.append("scientificName='").append(scientificName).append('\'');
        sb.append(", commonName='").append(commonName).append('\'');
        sb.append(", assembly='").append(assembly).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getScientificName() {
        return scientificName;
    }

    public ProjectOrganism setScientificName(String scientificName) {
        this.scientificName = scientificName;
        return this;
    }

    public String getCommonName() {
        return commonName;
    }

    public ProjectOrganism setCommonName(String commonName) {
        this.commonName = commonName;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public ProjectOrganism setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }
}
