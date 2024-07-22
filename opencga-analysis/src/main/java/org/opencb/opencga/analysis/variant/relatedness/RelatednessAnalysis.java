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

package org.opencb.opencga.analysis.variant.relatedness;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.family.qc.FamilyQcAnalysis;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.FamilyQcAnalysisParams;
import org.opencb.opencga.core.models.variant.RelatednessAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.IBDRelatednessAnalysisExecutor;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Tool(id = RelatednessAnalysis.ID, resource = Enums.Resource.VARIANT, description = RelatednessAnalysis.DESCRIPTION)
public class RelatednessAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "relatedness";
    public static final String DESCRIPTION = "Compute a score to quantify relatedness between samples.";

    public static final String MAF_DEFAULT_VALUE = "1000G:ALL>0.3";

//    private String familyId;
//    private List<String> individualIds;
//    private String method;
//    private String minorAlleleFreq;
//    private String haploidCallMode;
//    private Map<String, Map<String, Float>> thresholds;
//
//    private Family family;

    @ToolParams
    private RelatednessAnalysisParams relatednessParams = new RelatednessAnalysisParams();

    private String studyId;
    private List<String> sampleIds;
    private String minorAlleleFreq;
    private String haploidCallMode;
    private Map<String, Map<String, Float>> relatednessThresholds;

    private Family family;

    public RelatednessAnalysis() {
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study.");
        }

        try {
            studyId = catalogManager.getStudyManager().get(study, null, token).first().getFqn();
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

//        // Check family
//        if (StringUtils.isNotEmpty(relatednessParams.get)) {
//            family = IndividualQcUtils.getFamilyById(studyId, familyId, catalogManager, token);
//            if (family == null) {
//                throw new ToolException("Family '" + familyId + "' not found.");
//            }
//        }

        // Check individuals and samples
        if (CollectionUtils.isNotEmpty(relatednessParams.getIndividuals()) && CollectionUtils.isNotEmpty(relatednessParams.getSamples())) {
            throw new ToolException("Incorrect parameters: only a list of individuals or samples is allowed.");
        }

        if (CollectionUtils.isNotEmpty(relatednessParams.getIndividuals())) {
            // Check and get individual for each ID
            sampleIds = new ArrayList<>();
            for (String individualId : relatednessParams.getIndividuals()) {
                Sample sample = IndividualQcUtils.getValidSampleByIndividualId(studyId, individualId, catalogManager, token);
                sampleIds.add(sample.getId());
            }
        }

        if (CollectionUtils.isEmpty(sampleIds)) {
            throw new ToolException("Member samples not found to execute relatedness analysis.");
        }

        // Checking samples in family
        Set<String> familySet = new HashSet<>();
        for (String sampleId : sampleIds) {
            Family family = IndividualQcUtils.getFamilyBySampleId(studyId, sampleId, catalogManager, token);
            familySet.add(family.getId());
        }
        if (familySet.size() > 1) {
            throw new ToolException("More than one family found (" + StringUtils.join(familySet, ", ") + ") for the input samples ("
                    + StringUtils.join(sampleIds, ", ") + ")");
        }
        if (familySet.size() == 0) {
            throw new ToolException("No family found for the input samples (" + StringUtils.join(sampleIds, ", ") + ")");
        }
        family = IndividualQcUtils.getFamilyById(studyId, familySet.stream().collect(Collectors.toList()).get(0), catalogManager, token);

        // If the minor allele frequency is missing then set the default value
        minorAlleleFreq = relatednessParams.getMinorAlleleFreq();
        if (StringUtils.isEmpty(minorAlleleFreq)) {
            minorAlleleFreq = MAF_DEFAULT_VALUE;
        }

        // Check haploid call mode
        haploidCallMode = relatednessParams.getHaploidCallMode();
        if (StringUtils.isEmpty(haploidCallMode)) {
            haploidCallMode = RelatednessReport.HAPLOID_CALL_MODE_DEFAUT_VALUE;
        } else {
            switch (haploidCallMode) {
                case RelatednessReport.HAPLOID_CALL_MODE_HAPLOID_VALUE:
                case RelatednessReport.HAPLOID_CALL_MODE_MISSING_VALUE:
                case RelatednessReport.HAPLOID_CALL_MODE_REF_VALUE:
                    break;
                default:
                    throw new ToolException("Invalid haploid call value '" + haploidCallMode + "', accepted values are: "
                            + RelatednessReport.HAPLOID_CALL_MODE_HAPLOID_VALUE + ", " + RelatednessReport.HAPLOID_CALL_MODE_MISSING_VALUE
                            + " and " + RelatednessReport.HAPLOID_CALL_MODE_REF_VALUE);
            }
        }

        Path thresholdsPath = getOpencgaHome().resolve("analysis").resolve(FamilyQcAnalysis.ID).resolve("relatedness_thresholds.csv");
        relatednessThresholds = AnalysisUtils.parseRelatednessThresholds(thresholdsPath);
    }

    @Override
    protected void run() throws ToolException {

        step("relatedness", () -> {
            IBDRelatednessAnalysisExecutor relatednessExecutor = getToolExecutor(IBDRelatednessAnalysisExecutor.class);

            relatednessExecutor.setStudyId(studyId)
                    .setFamily(family)
                    .setSampleIds(sampleIds)
                    .setMinorAlleleFreq(minorAlleleFreq)
                    .setHaploidCallMode(haploidCallMode)
                    .setThresholds(relatednessThresholds)
                    .setResourcePath(getOpencgaHome().resolve("analysis/resources").resolve(ID))
                    .execute();
        });
    }
}
