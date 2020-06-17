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

package org.opencb.opencga.core.models.sample;

import java.io.Serializable;
import java.util.List;

public class QcReport implements Serializable {

    // Family ID
    private String familyId;

    // Father, mother and children IDs
    private String fatherId;
    private String motherId;
    private List<String> childrenIds;

    // Sex report
    private List<InferredSexReport> inferredSexReport;

    // Relatedness report
    private RelatednessReport relatednessReport;

    // Mendelian errors report
    private MendelianErrorReport mendelianErrorReport;

    // FastQC report
    private FastQcReport fastQcReport;

    // Flag stats report (from samtools flagstat)
    private FlagStatsReport flagStatsReport;

    // Hs metrics report (from picard/CollectHsMetrics tool)
    private HsMetricsReport hsMetricsReport;

    public QcReport() {
    }

    public QcReport(String familyId, String fatherId, String motherId, List<String> childrenIds, List<InferredSexReport> inferredSexReport,
                    RelatednessReport relatednessReport, MendelianErrorReport mendelianErrorReport) {
        this.familyId = familyId;
        this.fatherId = fatherId;
        this.motherId = motherId;
        this.childrenIds = childrenIds;
        this.inferredSexReport = inferredSexReport;
        this.relatednessReport = relatednessReport;
        this.mendelianErrorReport = mendelianErrorReport;
    }

    public String getFamilyId() {
        return familyId;
    }

    public QcReport setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public String getFatherId() {
        return fatherId;
    }

    public QcReport setFatherId(String fatherId) {
        this.fatherId = fatherId;
        return this;
    }

    public String getMotherId() {
        return motherId;
    }

    public QcReport setMotherId(String motherId) {
        this.motherId = motherId;
        return this;
    }

    public List<String> getChildrenIds() {
        return childrenIds;
    }

    public QcReport setChildrenIds(List<String> childrenIds) {
        this.childrenIds = childrenIds;
        return this;
    }

    public List<InferredSexReport> getInferredSexReport() {
        return inferredSexReport;
    }

    public QcReport setInferredSexReport(List<InferredSexReport> inferredSexReport) {
        this.inferredSexReport = inferredSexReport;
        return this;
    }

    public RelatednessReport getRelatednessReport() {
        return relatednessReport;
    }

    public QcReport setRelatednessReport(RelatednessReport relatednessReport) {
        this.relatednessReport = relatednessReport;
        return this;
    }

    public MendelianErrorReport getMendelianErrorReport() {
        return mendelianErrorReport;
    }

    public QcReport setMendelianErrorReport(MendelianErrorReport mendelianErrorReport) {
        this.mendelianErrorReport = mendelianErrorReport;
        return this;
    }
}
