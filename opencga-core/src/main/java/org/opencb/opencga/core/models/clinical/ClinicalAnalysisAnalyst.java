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

package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.common.TimeUtils;

public class ClinicalAnalysisAnalyst {

    private String assignee;
    private String assignedBy;
    private String date;

    public ClinicalAnalysisAnalyst() {
    }

    public ClinicalAnalysisAnalyst(String assignee, String assignedBy) {
        this.assignee = assignee;
        this.assignedBy = assignedBy;
        this.date = TimeUtils.getTime();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisAnalyst{");
        sb.append("assignee='").append(assignee).append('\'');
        sb.append(", assignedBy='").append(assignedBy).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAssignee() {
        return assignee;
    }

    public ClinicalAnalysisAnalyst setAssignee(String assignee) {
        this.assignee = assignee;
        return this;
    }

    public String getAssignedBy() {
        return assignedBy;
    }

    public ClinicalAnalysisAnalyst setAssignedBy(String assignedBy) {
        this.assignedBy = assignedBy;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalAnalysisAnalyst setDate(String date) {
        this.date = date;
        return this;
    }
}
