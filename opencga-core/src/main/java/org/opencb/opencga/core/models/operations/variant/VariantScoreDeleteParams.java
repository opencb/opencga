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

public class VariantScoreDeleteParams extends ToolParams {

    public static final String ID = "variant-score-delete";
    public static final String DESCRIPTION = "Remove a variant score in the database";
    private String scoreName;
    private boolean force;
    private boolean resume;

    public VariantScoreDeleteParams() {
    }

    public VariantScoreDeleteParams(String scoreName, boolean force, boolean resume) {
        this.scoreName = scoreName;
        this.force = force;
        this.resume = resume;
    }

    public String getScoreName() {
        return scoreName;
    }

    public VariantScoreDeleteParams setScoreName(String scoreName) {
        this.scoreName = scoreName;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public VariantScoreDeleteParams setForce(boolean force) {
        this.force = force;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantScoreDeleteParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }
}
