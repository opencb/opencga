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

import org.opencb.opencga.core.models.sample.RelatednessReport;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;

import java.util.List;

public class IndividualQualityControl {

    /**
     * Inferred Sex based on sexual chromosome coverage
     */
    private List<InferredSexReport> inferredSexReport;

    /**
     * Plink-based relatedness
     */
    private RelatednessReport relatednessReport;
    private MendelianErrorReport mendelianErrorReport;


    public IndividualQualityControl() {
    }

    public IndividualQualityControl(List<InferredSexReport> inferredSexReport, RelatednessReport relatednessReport, MendelianErrorReport mendelianErrorReport) {
        this.inferredSexReport = inferredSexReport;
        this.relatednessReport = relatednessReport;
        this.mendelianErrorReport = mendelianErrorReport;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQualityControl{");
        sb.append("inferredSexReport=").append(inferredSexReport);
        sb.append(", relatednessReport=").append(relatednessReport);
        sb.append(", mendelianErrorReport=").append(mendelianErrorReport);
        sb.append('}');
        return sb.toString();
    }

    public List<InferredSexReport> getInferredSexReport() {
        return inferredSexReport;
    }

    public IndividualQualityControl setInferredSexReport(List<InferredSexReport> inferredSexReport) {
        this.inferredSexReport = inferredSexReport;
        return this;
    }

    public RelatednessReport getRelatednessReport() {
        return relatednessReport;
    }

    public IndividualQualityControl setRelatednessReport(RelatednessReport relatednessReport) {
        this.relatednessReport = relatednessReport;
        return this;
    }

    public MendelianErrorReport getMendelianErrorReport() {
        return mendelianErrorReport;
    }

    public IndividualQualityControl setMendelianErrorReport(MendelianErrorReport mendelianErrorReport) {
        this.mendelianErrorReport = mendelianErrorReport;
        return this;
    }
}
