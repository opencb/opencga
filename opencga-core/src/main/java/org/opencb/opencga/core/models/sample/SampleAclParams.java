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
import org.opencb.opencga.core.models.AclParams;

import java.util.Objects;

// Acl params to communicate the WS and the sample manager
public class SampleAclParams extends AclParams {

    private String individual;
    private String family;
    private String file;
    private String cohort;

    public SampleAclParams() {
    }

    public SampleAclParams(String individual, String family, String file, String cohort, String permissions) {
        super(permissions);
        this.individual = individual;
        this.family = family;
        this.file = file;
        this.cohort = cohort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleAclParams{");
        sb.append("individual='").append(individual).append('\'');
        sb.append(", family='").append(family).append('\'');
        sb.append(", file='").append(file).append('\'');
        sb.append(", cohort='").append(cohort).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SampleAclParams that = (SampleAclParams) o;

        return new EqualsBuilder()
                .append(individual, that.individual)
                .append(family, that.family)
                .append(file, that.file)
                .append(cohort, that.cohort)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(individual, file, cohort);
    }

    public String getIndividual() {
        return individual;
    }

    public SampleAclParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public SampleAclParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getFile() {
        return file;
    }

    public SampleAclParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getCohort() {
        return cohort;
    }

    public SampleAclParams setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public SampleAclParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
