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
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.clinical.OpenCgaClinicalAnalysis;
import org.opencb.opencga.analysis.clinical.SecondaryFindingsAnalysis;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.ClinicalConsent;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class InterpretationAnalysis extends OpenCgaClinicalAnalysis<Interpretation> {

    public InterpretationAnalysis(String clinicalAnalysisId, String studyId, ObjectMap options, String opencgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, options, opencgaHome, sessionId);
    }

//    @Deprecated
//    public InterpretationAnalysis(String clinicalAnalysisId, Map<String, ClinicalProperty.RoleInCancer> roleInCancer, Map<String, List<String>> actionableVariants, ObjectMap config, String opencgaHome, String studyStr, String token) {
//        super(clinicalAnalysisId, roleInCancer, actionableVariants, config, opencgaHome, studyStr, token);
//    }

    @Override
    public abstract InterpretationResult execute() throws Exception;

    protected List<ReportedVariant> getSecondaryFindings(ClinicalAnalysis clinicalAnalysis,  List<String> sampleNames,
                                                         ReportedVariantCreator creator) throws Exception {
        List<ReportedVariant> secondaryFindings = null;
        if (clinicalAnalysis.getConsent() != null
                && clinicalAnalysis.getConsent().getSecondaryFindings() == ClinicalConsent.ConsentStatus.YES) {
//            List<Variant> findings = ClinicalUtils.secondaryFindings(studyId, sampleNames, actionableVariants.keySet(),
//                    excludeIds, variantStorageManager, token);
            SecondaryFindingsAnalysis secondaryFindingsAnalysis = new SecondaryFindingsAnalysis(sampleNames.get(0), clinicalAnalysisId,
                    studyId, null, opencgaHome, sessionId);
            List<Variant> variants = secondaryFindingsAnalysis.execute().getResult();
            if (CollectionUtils.isNotEmpty(variants)) {
                secondaryFindings = creator.createSecondaryFindings(variants);
            }
        }
        return secondaryFindings;
    }

    protected void addGenotypeFilter(Map<String, List<String>> genotypes, Map<String, String> sampleMap, Query query) {
        String genotypeString = StringUtils.join(genotypes.entrySet().stream()
                .filter(entry -> sampleMap.containsKey(entry.getKey()))
                .filter(entry -> ListUtils.isNotEmpty(entry.getValue()))
                .map(entry -> sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR))
                .collect(Collectors.toList()), ";");
        if (StringUtils.isNotEmpty(genotypeString)) {
            query.put(VariantQueryParam.GENOTYPE.key(), genotypeString);
        }
        logger.debug("Query: {}", query.toJson());
    }
}
