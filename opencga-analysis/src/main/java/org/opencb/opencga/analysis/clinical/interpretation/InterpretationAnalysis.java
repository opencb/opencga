/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis.clinical.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.clinical.OpenCgaClinicalAnalysis;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.manager.clinical.ClinicalUtils.readReportedVariants;

public abstract class InterpretationAnalysis extends OpenCgaClinicalAnalysis {

    public InterpretationAnalysis(String clinicalAnalysisId, String studyId, Path outDir, Path openCgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, outDir, openCgaHome, sessionId);
    }

    protected void saveResult(String analysisName, List<DiseasePanel> diseasePanels, ClinicalAnalysis clinicalAnalysis,
                              Query query, boolean includeLowCoverage, int maxLowCoverage) {
        // Reported low coverage regions
        List<ReportedLowCoverage> reportedLowCoverages = null;
        if (CollectionUtils.isNotEmpty(diseasePanels) && includeLowCoverage) {
            try {
                reportedLowCoverages = clinicalInterpretationManager.getReportedLowCoverage(maxLowCoverage, clinicalAnalysis,
                        diseasePanels, studyId, sessionId);
            } catch (AnalysisException e) {
                e.printStackTrace();
            }
        }

        // Software
        Software software = new Software().setName(analysisName);

        // Analyst
        Analyst analyst = null;
        try {
            analyst = clinicalInterpretationManager.getAnalyst(sessionId);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        List<ReportedVariant> primaryFindings = readReportedVariants(Paths.get(outDir.toString() + "/primary-findings.txt"));
        List<ReportedVariant> secondaryFindings = readReportedVariants(Paths.get(outDir.toString() + "/secondary-findings.txt"));

        Interpretation interpretation = new Interpretation()
                .setId(analysisName + "__" + TimeUtils.getTimeMillis())
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings)
                .setLowCoverageRegions(reportedLowCoverages)
                .setAnalyst(analyst)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(diseasePanels)
                .setFilters(query)
                .setSoftware(software);

        try {
            File file = Paths.get(outDir.toString() + "/interpretation.json").toFile();
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(file, interpretation);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
