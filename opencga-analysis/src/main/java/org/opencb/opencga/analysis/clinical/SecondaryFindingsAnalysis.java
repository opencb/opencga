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

package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class SecondaryFindingsAnalysis extends OpenCgaClinicalAnalysis<List<Variant>> {

    private String sampleId;

    public static final int BATCH_SIZE = 1000;


    public SecondaryFindingsAnalysis(String sampleId, String clinicalAnalysisId, String studyId, ObjectMap options, String opencgaHome,
                                     String sessionId) {
        super(clinicalAnalysisId, studyId, options, opencgaHome, sessionId);
        this.sampleId = sampleId;
    }


    @Override
    public AnalysisResult<List<Variant>> execute() throws Exception {
        // sampleId has preference over clinicalAnalysisId
        if (StringUtils.isEmpty(this.sampleId)) {
            // Throws an Exception if it cannot fetch analysis ID or proband is null
            ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis();
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getProband().getSamples())) {
                for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
                    if (!sample.isSomatic()) {
                        this.sampleId = clinicalAnalysis.getProband().getSamples().get(0).getId();
                        break;
                    }
                }
            } else {
                throw new AnalysisException("Missing germline sample");
            }
        }

        List<Variant> variants = new ArrayList<>();

        // Prepare query object
        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), studyId);
        query.put(VariantQueryParam.SAMPLE.key(), sampleId);

        // Get the correct actionable variants for the assembly
        String assembly = ClinicalUtils.getAssembly(catalogManager, studyId, sessionId);
        Map<String, List<String>> actionableVariants = actionableVariantManager.getActionableVariants(assembly);
        if (actionableVariants != null) {
            Iterator<String> iterator = actionableVariants.keySet().iterator();
            List<String> variantIds = new ArrayList<>();
            while (iterator.hasNext()) {
                String id = iterator.next();
                variantIds.add(id);
                if (variantIds.size() >= BATCH_SIZE) {
                    query.put(VariantQueryParam.ID.key(), variantIds);
                    VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), sessionId);
                    variants.addAll(result.getResult());
                    variantIds.clear();
                }
            }

            if (variantIds.size() > 0) {
                query.put(VariantQueryParam.ID.key(), variantIds);
                VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), sessionId);
                variants.addAll(result.getResult());
            }
        }

        return new AnalysisResult<>(variants);
    }
}
