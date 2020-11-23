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

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAlignmentQualityControlMetrics;
import org.opencb.opencga.core.models.sample.SampleVariantQualityControlMetrics;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class SampleQcAnalysisExecutor extends OpenCgaToolExecutor {

    public enum QcType {
        FLAG_STATS, HS_METRICS, GENE_COVERAGE_STATS
    }

    protected String studyId;
    protected Sample sample;
    protected File catalogBamFile;
    protected String dictFile;
    protected String baitFile;
    protected String variantStatsId;
    protected String variantStatsDecription;
    protected Query variantStatsQuery;
    protected String signatureId;
    protected Query signatureQuery;
    protected List<String> genesForCoverageStats;

    protected QcType qcType;

    protected SampleAlignmentQualityControlMetrics alignmentQcMetrics;

    public SampleQcAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public SampleQcAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public Sample getSample() {
        return sample;
    }

    public SampleQcAnalysisExecutor setSample(Sample sample) {
        this.sample = sample;
        return this;
    }

    public File getCatalogBamFile() {
        return catalogBamFile;
    }

    public SampleQcAnalysisExecutor setCatalogBamFile(File catalogBamFile) {
        this.catalogBamFile = catalogBamFile;
        return this;
    }

    public String getDictFile() {
        return dictFile;
    }

    public SampleQcAnalysisExecutor setDictFile(String dictFile) {
        this.dictFile = dictFile;
        return this;
    }

    public String getBaitFile() {
        return baitFile;
    }

    public SampleQcAnalysisExecutor setBaitFile(String baitFile) {
        this.baitFile = baitFile;
        return this;
    }

    public String getVariantStatsId() {
        return variantStatsId;
    }

    public SampleQcAnalysisExecutor setVariantStatsId(String variantStatsId) {
        this.variantStatsId = variantStatsId;
        return this;
    }

    public String getVariantStatsDecription() {
        return variantStatsDecription;
    }

    public SampleQcAnalysisExecutor setVariantStatsDecription(String variantStatsDecription) {
        this.variantStatsDecription = variantStatsDecription;
        return this;
    }

    public Query getVariantStatsQuery() {
        return variantStatsQuery;
    }

    public SampleQcAnalysisExecutor setVariantStatsQuery(Query variantStatsQuery) {
        this.variantStatsQuery = variantStatsQuery;
        return this;
    }

    public String getSignatureId() {
        return signatureId;
    }

    public SampleQcAnalysisExecutor setSignatureId(String signatureId) {
        this.signatureId = signatureId;
        return this;
    }

    public Query getSignatureQuery() {
        return signatureQuery;
    }

    public SampleQcAnalysisExecutor setSignatureQuery(Query signatureQuery) {
        this.signatureQuery = signatureQuery;
        return this;
    }

    public List<String> getGenesForCoverageStats() {
        return genesForCoverageStats;
    }

    public SampleQcAnalysisExecutor setGenesForCoverageStats(List<String> genesForCoverageStats) {
        this.genesForCoverageStats = genesForCoverageStats;
        return this;
    }

    public QcType getQcType() {
        return qcType;
    }

    public SampleQcAnalysisExecutor setQcType(QcType qcType) {
        this.qcType = qcType;
        return this;
    }

    public SampleAlignmentQualityControlMetrics getAlignmentQcMetrics() {
        return alignmentQcMetrics;
    }

    public SampleQcAnalysisExecutor setAlignmentQcMetrics(SampleAlignmentQualityControlMetrics alignmentQcMetrics) {
        this.alignmentQcMetrics = alignmentQcMetrics;
        return this;
    }
}
