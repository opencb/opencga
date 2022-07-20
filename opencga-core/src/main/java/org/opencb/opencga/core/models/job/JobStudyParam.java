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

package org.opencb.opencga.core.models.job;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.LinkedList;
import java.util.List;

import org.opencb.opencga.core.api.ParamConstants;

public class JobStudyParam {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "others", indexed = true,
            description = FieldConstants.JOB_STUDY_PARAM_OTHERS)
    private List<String> others;

    public JobStudyParam() {
    }

    public JobStudyParam(String id) {
        this(id, new LinkedList<>());
    }

    public JobStudyParam(String id, List<String> others) {
        this.id = id;
        this.others = others;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobRelatedStudies{");
        sb.append("id='").append(id).append('\'');
        sb.append(", others=").append(others);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public JobStudyParam setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getOthers() {
        return others;
    }

    public JobStudyParam setOthers(List<String> others) {
        this.others = others;
        return this;
    }
}
