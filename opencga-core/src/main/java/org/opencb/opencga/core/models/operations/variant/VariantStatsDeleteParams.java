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

package org.opencb.opencga.core.models.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantStatsDeleteParams extends ToolParams {
    public static final String DESCRIPTION = "Variant stats delete params";

    private List<String> cohort;
    private boolean force;
//    private boolean resume;

    public VariantStatsDeleteParams() {
    }

    public VariantStatsDeleteParams(List<String> cohort, boolean force) {
        this.cohort = cohort;
        this.force = force;
    }

    public List<String> getCohort() {
        return cohort;
    }

    public VariantStatsDeleteParams setCohort(List<String> cohort) {
        this.cohort = cohort;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public VariantStatsDeleteParams setForce(boolean force) {
        this.force = force;
        return this;
    }
}
