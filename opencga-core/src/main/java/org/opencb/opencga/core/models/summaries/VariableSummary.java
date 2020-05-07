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

package org.opencb.opencga.core.models.summaries;

import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 12/08/16.
 */
public class VariableSummary {

    private String name;
    private List<FeatureCount> annotations;

    public VariableSummary() {
        this("", Collections.emptyList());
    }

    public VariableSummary(String name, List<FeatureCount> annotations) {
        this.name = name;
        this.annotations = annotations;
    }

    public String getName() {
        return name;
    }

    public VariableSummary setName(String name) {
        this.name = name;
        return this;
    }

    public List<FeatureCount> getAnnotations() {
        return annotations;
    }

    public VariableSummary setAnnotations(List<FeatureCount> annotations) {
        this.annotations = annotations;
        return this;
    }
}
