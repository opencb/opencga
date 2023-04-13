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

package org.opencb.opencga.core.tools.family;

import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

public abstract class PedigreeGraphAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private Family family;

    public PedigreeGraphAnalysisExecutor() {
    }

    public String getStudy() {
        return study;
    }

    public PedigreeGraphAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Family getFamily() {
        return family;
    }

    public PedigreeGraphAnalysisExecutor setFamily(Family family) {
        this.family = family;
        return this;
    }
}
