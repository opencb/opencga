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

package org.opencb.opencga.core.models.individual;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class IndividualPopulation {


    @DataField(id = "name", indexed = true, description = FieldConstants.INDIVIDUAL_POPULATION_NAME)
    private String name;
    @DataField(id = "subpopulation", indexed = true, description = FieldConstants.INDIVIDUAL_POPULATION_SUBPOPULATION)
    private String subpopulation;
    @DataField(id = "description", indexed = true, description = FieldConstants.INDIVIDUAL_POPULATION_DESCRIPTION)
    private String description;


    public IndividualPopulation() {
    }

    public IndividualPopulation(String name, String subpopulation, String description) {
        this.name = name;
        this.subpopulation = subpopulation;
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualPopulation{");
        sb.append("name='").append(name).append('\'');
        sb.append(", subpopulation='").append(subpopulation).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public IndividualPopulation setName(String name) {
        this.name = name;
        return this;
    }

    public String getSubpopulation() {
        return subpopulation;
    }

    public IndividualPopulation setSubpopulation(String subpopulation) {
        this.subpopulation = subpopulation;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public IndividualPopulation setDescription(String description) {
        this.description = description;
        return this;
    }
}
