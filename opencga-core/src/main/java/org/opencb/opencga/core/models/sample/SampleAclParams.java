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

import org.opencb.opencga.core.models.AclParams;

import java.util.Objects;

// Acl params to communicate the WS and the sample manager
public class SampleAclParams extends AclParams {

    private String individual;
    private String file;
    private String cohort;
    private boolean propagate;

    public SampleAclParams() {
    }

    public SampleAclParams(String permissions, Action action, String individual, String file, String cohort) {
        super(permissions, action);
        this.individual = individual;
        this.file = file;
        this.cohort = cohort;
        this.propagate = false;
    }

    public SampleAclParams(String permissions, Action action, String individual, String file, String cohort, boolean propagate) {
        super(permissions, action);
        this.individual = individual;
        this.file = file;
        this.cohort = cohort;
        this.propagate = propagate;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleAclParams{");
        sb.append("permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append(", individual='").append(individual).append('\'');
        sb.append(", file='").append(file).append('\'');
        sb.append(", cohort='").append(cohort).append('\'');
        sb.append(", propagate=").append(propagate);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SampleAclParams)) {
            return false;
        }
        SampleAclParams that = (SampleAclParams) o;
        return Objects.equals(individual, that.individual)
                && Objects.equals(file, that.file)
                && Objects.equals(cohort, that.cohort);
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

    public boolean isPropagate() {
        return propagate;
    }

    public SampleAclParams setPropagate(boolean propagate) {
        this.propagate = propagate;
        return this;
    }

    public SampleAclParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

    public SampleAclParams setAction(Action action) {
        super.setAction(action);
        return this;
    }
}
