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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ReportedEvent;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;

import java.util.*;
import java.util.stream.Collectors;

public abstract class FamilyInterpretationAnalysis extends InterpretationAnalysis {

    protected List<String> diseasePanelIds;

    @Deprecated
    protected final static String SEPARATOR = "__";

    public final static String SKIP_DIAGNOSTIC_VARIANTS_PARAM = "skipDiagnosticVariants";
    public final static String SKIP_UNTIERED_VARIANTS_PARAM = "skipUntieredVariants";


    public FamilyInterpretationAnalysis(String clinicalAnalysisId, String studyId, List<String> diseasePanelIds, ObjectMap options,
                                        String opencgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, options, opencgaHome, sessionId);
        this.diseasePanelIds = diseasePanelIds;
    }


    @Deprecated
    protected ClinicalAnalysis getClinicalAnalysis() throws AnalysisException {
        ClinicalAnalysis clinicalAnalysis = super.getClinicalAnalysis();

        // Sanity checks
        if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
            throw new AnalysisException("Missing family in clinical analysis " + clinicalAnalysisId);
        }

        return clinicalAnalysis;
    }

    protected Individual getProband(ClinicalAnalysis clinicalAnalysis) throws AnalysisException {
        Individual proband = super.getProband(clinicalAnalysis);

        // Sanity check
        if (proband.getSamples().size() > 1) {
            throw new AnalysisException("Found more than one sample for proband " + proband.getId() + " in clinical analysis "
                    + clinicalAnalysisId);
        }

        return proband;
    }

        // Family
    protected List<ReportedVariant> getCompoundHeterozygousReportedVariants(Map<String, List<Variant>> chVariantMap,
                                                                            ReportedVariantCreator creator)
            throws InterpretationAnalysisException {
        // Compound heterozygous management
        // Create transcript - reported variant map from transcript - variant
        Map<String, List<ReportedVariant>> reportedVariantMap = new HashMap<>();
        for (Map.Entry<String, List<Variant>> entry : chVariantMap.entrySet()) {
            reportedVariantMap.put(entry.getKey(), creator.createReportedVariants(entry.getValue()));
        }
        return groupCHVariants(reportedVariantMap);
    }

    public List<ReportedVariant> groupCHVariants(Map<String, List<ReportedVariant>> reportedVariantMap) {
        List<ReportedVariant> reportedVariants = new ArrayList<>();

        for (Map.Entry<String, List<ReportedVariant>> entry : reportedVariantMap.entrySet()) {
            Set<String> variantIds = entry.getValue().stream().map(Variant::toStringSimple).collect(Collectors.toSet());
            for (ReportedVariant reportedVariant : entry.getValue()) {
                Set<String> tmpVariantIds = new HashSet<>(variantIds);
                tmpVariantIds.remove(reportedVariant.toStringSimple());

                for (ReportedEvent reportedEvent : reportedVariant.getEvidences()) {
                    reportedEvent.setCompoundHeterozygousVariantIds(new ArrayList<>(tmpVariantIds));
                }

                reportedVariants.add(reportedVariant);
            }
        }

        return reportedVariants;
    }




}
