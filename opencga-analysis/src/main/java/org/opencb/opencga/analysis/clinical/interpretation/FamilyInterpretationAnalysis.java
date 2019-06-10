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
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.RoleInCancer;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.core.models.ClinicalAnalysis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS;

public abstract class FamilyInterpretationAnalysis extends InterpretationAnalysis {

    protected List<String> diseasePanelIds;

    @Deprecated
    protected final static String SEPARATOR = "__";

    public final static String SKIP_DIAGNOSTIC_VARIANTS_PARAM = "skipDiagnosticVariants";
    public final static String SKIP_UNTIERED_VARIANTS_PARAM = "skipUntieredVariants";

//    protected static Set<String> extendedLof;
//    protected static Set<String> proteinCoding;
//
//    static {
//        proteinCoding = new HashSet<>(Arrays.asList("protein_coding", "IG_C_gene", "IG_D_gene", "IG_J_gene", "IG_V_gene",
//                "nonsense_mediated_decay", "non_stop_decay", "TR_C_gene", "TR_D_gene", "TR_J_gene", "TR_V_gene"));
//
//        extendedLof = new HashSet<>(Arrays.asList("SO:0001893", "SO:0001574", "SO:0001575", "SO:0001587", "SO:0001589", "SO:0001578",
//                "SO:0001582", "SO:0001889", "SO:0001821", "SO:0001822", "SO:0001583", "SO:0001630", "SO:0001626"));
//    }

    public FamilyInterpretationAnalysis(String clinicalAnalysisId, String studyId, List<String> diseasePanelIds, ObjectMap options,
                                        String opencgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, options, opencgaHome, sessionId);
        this.diseasePanelIds = diseasePanelIds;
    }

//    @Deprecated
//    public FamilyInterpretationAnalysis(String clinicalAnalysisId, List<String> diseasePanelIds, Map<String, RoleInCancer> roleInCancer,
//                                        Map<String, List<String>> actionableVariants, ClinicalProperty.Penetrance penetrance, ObjectMap config,
//                                        String studyId, String opencgaHome, String token) {
//        super(clinicalAnalysisId, roleInCancer, actionableVariants, config, opencgaHome, studyId, token);
//
//        this.diseasePanelIds = diseasePanelIds;
//
//        this.penetrance = penetrance;
//    }

    @Deprecated
    protected ClinicalAnalysis getClinicalAnalysis() throws AnalysisException {
        ClinicalAnalysis clinicalAnalysis = super.getClinicalAnalysis();

        // Sanity checks
        if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
            throw new AnalysisException("Missing family in clinical analysis " + clinicalAnalysisId);
        }

        return clinicalAnalysis;
    }

    // Family
    protected List<ReportedVariant> getCompoundHeterozygousReportedVariants(Map<String, List<Variant>> chVariantMap,
                                                                            ReportedVariantCreator creator)
            throws InterpretationAnalysisException {
        // Compound heterozygous management
        // Create transcript - reported variant map from transcript - variant
        Map<String, List<ReportedVariant>> reportedVariantMap = new HashMap<>();
        for (Map.Entry<String, List<Variant>> entry : chVariantMap.entrySet()) {
            reportedVariantMap.put(entry.getKey(), creator.create(entry.getValue(), COMPOUND_HETEROZYGOUS));
        }
        return creator.groupCHVariants(reportedVariantMap);
    }

    // Family



}
