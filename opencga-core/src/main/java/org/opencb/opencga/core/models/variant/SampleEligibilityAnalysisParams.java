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

package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class SampleEligibilityAnalysisParams extends ToolParams {

    public static final String DESCRIPTION = "";
    private String query;
    private boolean index;
    private String cohortId;

    public SampleEligibilityAnalysisParams() {
    }

    public SampleEligibilityAnalysisParams(String query, boolean index, String cohortId) {
        this.query = query;
        this.index = index;
        this.cohortId = cohortId;
    }

    public String getQuery() {
        return query;
    }

    public SampleEligibilityAnalysisParams setQuery(String query) {
        this.query = query;
        return this;
    }

    public boolean isIndex() {
        return index;
    }

    public SampleEligibilityAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }

    public String getCohortId() {
        return cohortId;
    }

    public SampleEligibilityAnalysisParams setCohortId(String cohortId) {
        this.cohortId = cohortId;
        return this;
    }
}
