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

import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.commons.datastore.core.ObjectMap;

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

    public FamilyInterpretationAnalysis(String clinicalAnalysisId, String studyId, List<String> diseasePanelIds, ObjectMap options,
                                        String opencgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, options, opencgaHome, sessionId);
        this.diseasePanelIds = diseasePanelIds;
    }

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
}
