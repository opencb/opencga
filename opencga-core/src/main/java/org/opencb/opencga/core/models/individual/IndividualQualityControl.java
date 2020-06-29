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

package org.opencb.opencga.core.models.individual;

import org.opencb.biodata.models.clinical.Comment;

import java.util.List;

public class IndividualQualityControl {

    /**
     * List of metrics for that individual, one metrics per sample
     */
    private List<IndividualQualityControlMetrics> metrics;

    /**
     * Comments related to the quality control
     */
    private List<Comment> comments;

    public IndividualQualityControl() {
    }

    public IndividualQualityControl(List<IndividualQualityControlMetrics> metrics, List<Comment> comments) {
        this.metrics = metrics;
        this.comments = comments;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQualityControl{");
        sb.append("metrics=").append(metrics);
        sb.append(", comments=").append(comments);
        sb.append('}');
        return sb.toString();
    }

    public List<IndividualQualityControlMetrics> getMetrics() {
        return metrics;
    }

    public IndividualQualityControl setMetrics(List<IndividualQualityControlMetrics> metrics) {
        this.metrics = metrics;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public IndividualQualityControl setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }
}
