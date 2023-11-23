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

package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.variant.CircosAnalysisParams;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

public abstract class CircosAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private CircosAnalysisParams circosParams;

    public CircosAnalysisExecutor() {
    }

    public CircosAnalysisExecutor(String study, CircosAnalysisParams circosParams) {
        this.study = study;
        this.circosParams = circosParams;
    }

    public String getStudy() {
        return study;
    }

    public CircosAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public CircosAnalysisParams getCircosParams() {
        return circosParams;
    }

    public CircosAnalysisExecutor setCircosParams(CircosAnalysisParams params) {
        this.circosParams = params;
        return this;
    }
}
