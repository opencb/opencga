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

    /**
     * We keep a Map for each assembly with a Map of variant IDs with the phenotype list
     */
    private static Map<String, Map<String, List<String>>> actionableVariants;

    public static final int BATCH_SIZE = 1000;


    public SecondaryFindingsAnalysis(String sampleId, String clinicalAnalysisId, String studyId, ObjectMap config, String opencgaHome,
                                     String token) {
        super(clinicalAnalysisId, studyId, config, opencgaHome, token);

        this.sampleId = sampleId;
    }


    @Override
    public AnalysisResult<List<Variant>> execute() throws Exception {
        if (actionableVariants == null || actionableVariants.isEmpty()) {
            actionableVariants = getActionableVariantsByAssembly();
        }

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

        // Prepare query object
        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), studyId);
        query.put(VariantQueryParam.SAMPLE.key(), sampleId);

        // Get the correct actionable variants for the assembly
        String assembly = ClinicalUtils.getAssembly(catalogManager, studyId, token);
        Map<String, List<String>> actionableVariantsByAssembly = actionableVariants.get(assembly);

        List<Variant> variants = new ArrayList<>();
        if (actionableVariantsByAssembly != null) {
            Iterator<String> iterator = actionableVariantsByAssembly.keySet().iterator();
            List<String> variantIds = new ArrayList<>();
            while (iterator.hasNext()) {
                String id = iterator.next();
                variantIds.add(id);
                if (variantIds.size() >= BATCH_SIZE) {
                    query.put(VariantQueryParam.ID.key(), variantIds);
                    VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), token);
                    variants.addAll(result.getResult());
                    variantIds.clear();
                }
            }

            if (variantIds.size() > 0) {
                query.put(VariantQueryParam.ID.key(), variantIds);
                VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), token);
                variants.addAll(result.getResult());
            }
        }

        AnalysisResult<List<Variant>> listAnalysisResult = new AnalysisResult<>(variants);

        return listAnalysisResult;
    }

    private Map<String, Map<String, List<String>>> getActionableVariantsByAssembly() throws IOException {
        // Load actionable variants for each assembly, if present
        // First, read all actionableVariants filenames, actionableVariants_xxx.txt[.gz] where xxx = assembly in lower case
        Map<String, Map<String, List<String>>> actionableVariantsByAssembly = new HashMap<>();
        File folder = Paths.get(opencgaHome + "/analysis/resources/").toFile();
        File[] files = folder.listFiles();
        if (files != null && ArrayUtils.isNotEmpty(files)) {
            for (File file : files) {
                if (file.isFile() && file.getName().startsWith("actionableVariants_")) {
                    // Split by _ and .  to fetch assembly
                    String[] split = file.getName().split("[_\\.]");
                    if (split.length > 1) {
                        String assembly = split[1].toLowerCase();
                        actionableVariantsByAssembly.put(assembly, loadActionableVariants(file));
                    }
                }
            }
        }
        return actionableVariantsByAssembly;
    }

    /**
     * Returns a Map of variant IDs with a alist of phenotypes.
     * @param file OpenCGA home installation folder
     * @return Map of variant IDs with a alist of phenotypes
     * @throws IOException If file is not found
     */
    private Map<String, List<String>> loadActionableVariants(File file) throws IOException {
        Map<String, List<String>> actionableVariants = new HashMap<>();

        if (file != null && file.exists()) {
            BufferedReader bufferedReader = FileUtils.newBufferedReader(file.toPath());
            List<String> lines = bufferedReader.lines().collect(Collectors.toList());
            for (String line : lines) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] split = line.split("\t");
                if (split.length > 4) {
                    List<String> phenotypes = new ArrayList<>();
                    if (split.length > 8 && StringUtils.isNotEmpty(split[8])) {
                        phenotypes.addAll(Arrays.asList(split[8].split(";")));
                    }
                    try {
                        Variant variant = new VariantBuilder(split[0], Integer.parseInt(split[1]), Integer.parseInt(split[2]), split[3],
                                split[4]).build();
                        actionableVariants.put(variant.toString(), phenotypes);
                    } catch (NumberFormatException e) {
                        // Skip this variant
                        System.err.println("Skip actionable variant: " + line + "\nCause: " + e.getMessage());
                    }
                } else {
                    // Skip this variant
                    System.err.println("Skip actionable variant, invalid format: " + line);
                }
            }
        }

        return actionableVariants;
    }
}
