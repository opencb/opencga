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

public class VariantStorageMetadataRepairToolParams extends ToolParams {

    public static final String DESCRIPTION = "Variant storage metadata repair params.";

    private List<String> studies;
    private int samplesBatchSize = 1000;
    private List<What> what;

    public enum What {
        SAMPLE_FILE_ID,
        CHECK_COHORT_ALL,
        REPAIR_COHORT_ALL,
    }

    public List<String> getStudies() {
        return studies;
    }

    public VariantStorageMetadataRepairToolParams setStudies(List<String> studies) {
        this.studies = studies;
        return this;
    }

    public int getSamplesBatchSize() {
        return samplesBatchSize;
    }

    public void setSamplesBatchSize(int samplesBatchSize) {
        this.samplesBatchSize = samplesBatchSize;
    }

    public List<What> getWhat() {
        return what;
    }

    public VariantStorageMetadataRepairToolParams setWhat(List<What> what) {
        this.what = what;
        return this;
    }
}
