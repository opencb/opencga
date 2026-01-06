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

package org.opencb.opencga.analysis.clinical.interpreter.config;

public class ExecutionInterpreterConfiguration {

    private String id;
    private String logic;
    private String tier;
    private boolean secondaryFinding;

    public ExecutionInterpreterConfiguration() {
    }

    public ExecutionInterpreterConfiguration(String id, String logic) {
        this.id = id;
        this.logic = logic;
    }

    public ExecutionInterpreterConfiguration(String id, String logic, String tier, boolean secondaryFinding) {
        this.id = id;
        this.logic = logic;
        this.tier = tier;
        this.secondaryFinding = secondaryFinding;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionInterpreterConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", logic='").append(logic).append('\'');
        sb.append(", tier='").append(tier).append('\'');
        sb.append(", secondaryFinding=").append(secondaryFinding);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ExecutionInterpreterConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public String getLogic() {
        return logic;
    }

    public ExecutionInterpreterConfiguration setLogic(String logic) {
        this.logic = logic;
        return this;
    }

    public String getTier() {
        return tier;
    }

    public ExecutionInterpreterConfiguration setTier(String tier) {
        this.tier = tier;
        return this;
    }

    public boolean isSecondaryFinding() {
        return secondaryFinding;
    }

    public ExecutionInterpreterConfiguration setSecondaryFinding(boolean secondaryFinding) {
        this.secondaryFinding = secondaryFinding;
        return this;
    }
}
