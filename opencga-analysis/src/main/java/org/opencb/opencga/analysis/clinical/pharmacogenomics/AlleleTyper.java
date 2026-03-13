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

package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * AlleleTyper processes pharmacogenomics genotyping data and calls diplotypes
 * by testing all pairs of haplotypes against observed diploid genotypes.
 */
public class AlleleTyper {

    private Logger logger = LoggerFactory.getLogger(AlleleTyper.class);

    // Parsed translation data: gene -> list of haplotype definitions
    private Map<String, List<HaplotypeDefinition>> geneHaplotypes;

    // gene -> set of assay IDs that have at least one non-empty value across all haplotypes
    private Map<String, Set<String>> geneRelevantAssays;

    // Column names ending with _cn (CNV columns from translation file)
    private Set<String> cnvColumnNames;

    // Assay ID to rsID mapping
    private Map<String, String> assayToRsIdMap;

    // Assay ID to column index in the translation file
    private Map<String, Integer> assayToColumnMap;

    // CNV data: sample -> target -> CN Predicted value
    private Map<String, Map<String, Integer>> sampleCnvData;

    public AlleleTyper() {
        this.geneHaplotypes = new HashMap<>();
        this.geneRelevantAssays = new HashMap<>();
        this.cnvColumnNames = new LinkedHashSet<>();
        this.assayToRsIdMap = new HashMap<>();
        this.assayToColumnMap = new HashMap<>();
        this.sampleCnvData = new HashMap<>();
    }

    // -----------------------------------------------------------------------
    // Translation parsing
    // -----------------------------------------------------------------------

    public void parseTranslationFromString(String translationContent) throws IOException {
        logger.info("Parsing translation content from String");
        try (BufferedReader br = new BufferedReader(new StringReader(translationContent))) {
            parseTranslationFromReader(br);
        }
    }

    public void parseTranslationFile(Path translationFile) throws IOException {
        logger.info("Parsing translation file: {}", translationFile);
        try (BufferedReader br = new BufferedReader(new FileReader(translationFile.toFile()))) {
            parseTranslationFromReader(br);
        }
    }

    private void parseTranslationFromReader(BufferedReader br) throws IOException {
        String line;
        int lineNumber = 0;
        String[] rsIds = null;
        String[] assayIds = null;

        while ((line = br.readLine()) != null) {
            lineNumber++;

            // Skip first line (RUO notice)
            if (lineNumber == 1) {
                continue;
            }

            String[] fields = line.split("\t", -1);

            // Line 2: rsIDs header
            if (lineNumber == 2) {
                rsIds = fields;
                continue;
            }

            // Line 3: Assay IDs and column headers
            if (lineNumber == 3) {
                assayIds = fields;
                buildAssayMaps(assayIds, rsIds);
                continue;
            }

            // Line 4+: Star allele definitions
            if (fields.length >= 3) {
                parseHaplotypeDefinition(fields, assayIds);
            }
        }

        // Compute relevant assays per gene
        computeGeneRelevantAssays();

        logger.info("Parsed {} genes with haplotype definitions, {} CNV columns detected",
                geneHaplotypes.size(), cnvColumnNames.size());
    }

    private void buildAssayMaps(String[] assayIds, String[] rsIds) {
        if (assayIds == null || rsIds == null) {
            return;
        }

        int minLength = Math.min(assayIds.length, rsIds.length);
        for (int i = 0; i < minLength; i++) {
            String assayId = assayIds[i].trim();
            String rsId = rsIds[i].trim();

            boolean isCnvColumn = assayId.toLowerCase().endsWith("_cn");
            boolean isAssayColumn = assayId.startsWith("C_") || assayId.startsWith("ANGZ")
                    || assayId.startsWith("ANM") || assayId.startsWith("ANER");

            if (StringUtils.isNotEmpty(assayId) && (isAssayColumn || isCnvColumn)) {
                assayToColumnMap.put(assayId, i);

                if (StringUtils.isNotEmpty(rsId)) {
                    assayToRsIdMap.put(assayId, rsId);
                }

                if (isCnvColumn) {
                    cnvColumnNames.add(assayId);
                }
            }
        }

        logger.info("Mapped {} assays to columns, {} are CNV columns",
                assayToColumnMap.size(), cnvColumnNames.size());
    }

    private void parseHaplotypeDefinition(String[] fields, String[] assayIds) {
        if (fields.length < 3) {
            return;
        }

        String gene = fields[0].trim();
        String allele = fields[1].trim();

        if (StringUtils.isEmpty(gene) || StringUtils.isEmpty(allele)) {
            return;
        }

        Map<String, String> assayValues = new LinkedHashMap<>();

        for (Map.Entry<String, Integer> entry : assayToColumnMap.entrySet()) {
            String assayId = entry.getKey();
            int columnIndex = entry.getValue();

            if (columnIndex < fields.length) {
                String value = fields[columnIndex].trim();
                // Store all values including empty ones — empty means wildcard
                assayValues.put(assayId, value);
            }
        }

        HaplotypeDefinition haplotype = new HaplotypeDefinition(gene, allele, assayValues);
        geneHaplotypes.computeIfAbsent(gene, k -> new ArrayList<>()).add(haplotype);
    }

    /**
     * Compute the set of relevant assays per gene: those with at least one non-empty value
     * across all haplotype definitions for that gene.
     */
    private void computeGeneRelevantAssays() {
        for (Map.Entry<String, List<HaplotypeDefinition>> entry : geneHaplotypes.entrySet()) {
            String gene = entry.getKey();
            Set<String> relevant = new LinkedHashSet<>();

            for (HaplotypeDefinition hap : entry.getValue()) {
                for (Map.Entry<String, String> assayEntry : hap.assayValues.entrySet()) {
                    if (StringUtils.isNotEmpty(assayEntry.getValue())) {
                        relevant.add(assayEntry.getKey());
                    }
                }
            }

            geneRelevantAssays.put(gene, relevant);
        }
    }

    // -----------------------------------------------------------------------
    // CNV file parsing
    // -----------------------------------------------------------------------

    public void parseCnvFromString(String cnvContent) throws IOException {
        if (StringUtils.isEmpty(cnvContent)) {
            return;
        }
        logger.info("Parsing CNV content from String");
        try (BufferedReader br = new BufferedReader(new StringReader(cnvContent))) {
            parseCnvFromReader(br);
        }
    }

    public void parseCnvFile(Path cnvFile) throws IOException {
        logger.info("Parsing CNV file: {}", cnvFile);
        try (BufferedReader br = new BufferedReader(new FileReader(cnvFile.toFile()))) {
            parseCnvFromReader(br);
        }
    }

    /**
     * Parse multi-section CNV file. Each section has a header row containing
     * "Sample Name" and "CN Predicted" columns among others, followed by data rows.
     * A new section starts when a new header row with "Sample Name" is encountered.
     */
    private void parseCnvFromReader(BufferedReader br) throws IOException {
        String line;
        int sampleNameIndex = -1;
        int cnPredictedIndex = -1;
        int targetIndex = -1;
        boolean headerParsed = false;

        while ((line = br.readLine()) != null) {
            if (line.trim().isEmpty()) {
                // Empty line may signal a new section
                headerParsed = false;
                continue;
            }

            String[] fields = line.split("\t", -1);

            // Check if this line is a header row
            if (!headerParsed) {
                sampleNameIndex = -1;
                cnPredictedIndex = -1;
                targetIndex = -1;

                for (int i = 0; i < fields.length; i++) {
                    String header = fields[i].trim();
                    if ("Sample Name".equals(header)) {
                        sampleNameIndex = i;
                    } else if ("CN Predicted".equals(header)) {
                        cnPredictedIndex = i;
                    } else if ("Target".equals(header)) {
                        targetIndex = i;
                    }
                }

                if (sampleNameIndex >= 0 && cnPredictedIndex >= 0) {
                    headerParsed = true;
                    continue;
                }
                // Not a header line, skip
                continue;
            }

            // Data row
            if (sampleNameIndex < fields.length && cnPredictedIndex < fields.length) {
                String sampleName = fields[sampleNameIndex].trim();
                String cnPredictedStr = fields[cnPredictedIndex].trim();
                String target = (targetIndex >= 0 && targetIndex < fields.length)
                        ? fields[targetIndex].trim() : "";

                if (StringUtils.isNotEmpty(sampleName) && StringUtils.isNotEmpty(cnPredictedStr)) {
                    try {
                        int cnPredicted = Integer.parseInt(cnPredictedStr);
                        sampleCnvData.computeIfAbsent(sampleName, k -> new HashMap<>())
                                .put(target, cnPredicted);
                    } catch (NumberFormatException e) {
                        // Skip non-integer CN values
                        logger.debug("Non-integer CN Predicted value for sample {}: {}", sampleName, cnPredictedStr);
                    }
                }
            }
        }

        logger.info("Parsed CNV data for {} samples", sampleCnvData.size());
    }

    // -----------------------------------------------------------------------
    // Genotyping file parsing and result building
    // -----------------------------------------------------------------------

    public List<AlleleTyperResult> buildAlleleTyperResultsFromString(String genotypingContent) throws IOException {
        try (BufferedReader br = new BufferedReader(new StringReader(genotypingContent))) {
            return buildAlleleTyperResultsFromReader(br);
        }
    }

    public List<AlleleTyperResult> buildAlleleTyperResults(Path genotypingFile) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(genotypingFile.toFile()))) {
            return buildAlleleTyperResultsFromReader(br);
        }
    }

    private List<AlleleTyperResult> buildAlleleTyperResultsFromReader(BufferedReader br) throws IOException {
        // Parse genotypes: sample -> assay -> call
        Map<String, Map<String, String>> sampleGenotypes = parseGenotypesFromReader(br);

        logger.info("Parsed genotypes for {} samples", sampleGenotypes.size());

        // Build results for each sample
        List<AlleleTyperResult> results = new ArrayList<>();

        for (Map.Entry<String, Map<String, String>> entry : sampleGenotypes.entrySet()) {
            String sampleId = entry.getKey();
            Map<String, String> genotypes = entry.getValue();
            Map<String, Integer> cnvData = sampleCnvData.getOrDefault(sampleId, Collections.emptyMap());

            AlleleTyperResult result = buildSampleResult(sampleId, genotypes, cnvData);
            results.add(result);
        }

        return results;
    }

    private Map<String, Map<String, String>> parseGenotypesFromReader(BufferedReader br) throws IOException {
        Map<String, Map<String, String>> sampleGenotypes = new LinkedHashMap<>();
        String line;
        boolean headerParsed = false;
        int sampleIdIndex = -1;
        int assayNameIndex = -1;
        int callIndex = -1;

        while ((line = br.readLine()) != null) {
            if (line.startsWith("#") || line.trim().isEmpty()) {
                continue;
            }

            String[] fields = line.split("\t", -1);

            if (!headerParsed) {
                for (int i = 0; i < fields.length; i++) {
                    String header = fields[i].trim();
                    if ("Sample ID".equals(header)) {
                        sampleIdIndex = i;
                    } else if ("Assay Name".equals(header)) {
                        assayNameIndex = i;
                    } else if ("Call".equals(header)) {
                        callIndex = i;
                    }
                }
                headerParsed = true;
                continue;
            }

            if (sampleIdIndex >= 0 && assayNameIndex >= 0 && callIndex >= 0
                    && sampleIdIndex < fields.length && assayNameIndex < fields.length && callIndex < fields.length) {
                String sampleId = fields[sampleIdIndex].trim();
                String assayName = fields[assayNameIndex].trim();
                String call = fields[callIndex].trim();

                if (StringUtils.isNotEmpty(sampleId) && StringUtils.isNotEmpty(assayName)) {
                    sampleGenotypes.computeIfAbsent(sampleId, k -> new LinkedHashMap<>()).put(assayName, call);
                }
            }
        }

        return sampleGenotypes;
    }

    private AlleleTyperResult buildSampleResult(String sampleId, Map<String, String> genotypes,
                                                Map<String, Integer> cnvData) {
        List<AlleleTyperResult.StarAlleleResult> alleleTyperResults = new ArrayList<>();

        for (Map.Entry<String, List<HaplotypeDefinition>> geneEntry : geneHaplotypes.entrySet()) {
            String gene = geneEntry.getKey();
            List<HaplotypeDefinition> haplotypes = geneEntry.getValue();
            Set<String> relevantAssays = geneRelevantAssays.getOrDefault(gene, Collections.emptySet());
            List<String> geneVariants = new ArrayList<>(relevantAssays);

            // Get all compatible diplotype pairs for this gene
            List<DiplotypePair> pairs = callDiplotype(gene, haplotypes, relevantAssays, genotypes, cnvData);

            if (pairs.isEmpty()) {
                alleleTyperResults.add(new AlleleTyperResult.StarAlleleResult(
                        gene, "no translation available", Collections.emptyList(), geneVariants));
            } else {
                // One StarAlleleResult per compatible pair
                for (DiplotypePair pair : pairs) {
                    List<AlleleTyperResult.AlleleCall> alleleCalls = new ArrayList<>();
                    alleleCalls.add(new AlleleTyperResult.AlleleCall(pair.h1.allele));
                    alleleCalls.add(new AlleleTyperResult.AlleleCall(pair.h2.allele));
                    alleleTyperResults.add(new AlleleTyperResult.StarAlleleResult(
                            gene, pair.toString(), alleleCalls, geneVariants));
                }
            }
        }

        // Build genotype list
        List<AlleleTyperResult.Genotype> genotypeList = new ArrayList<>();
        for (Map.Entry<String, String> genoEntry : genotypes.entrySet()) {
            genotypeList.add(new AlleleTyperResult.Genotype(genoEntry.getKey(), genoEntry.getValue()));
        }

        // Build translation info
        List<AlleleTyperResult.TranslationInfo> translationList = buildTranslationInfo();

        return new AlleleTyperResult(sampleId, alleleTyperResults, genotypeList, translationList);
    }

    private List<AlleleTyperResult.TranslationInfo> buildTranslationInfo() {
        List<AlleleTyperResult.TranslationInfo> translationList = new ArrayList<>();

        for (Map.Entry<String, List<HaplotypeDefinition>> entry : geneHaplotypes.entrySet()) {
            String gene = entry.getKey();

            Map<String, String> geneAssays = new LinkedHashMap<>();
            for (HaplotypeDefinition def : entry.getValue()) {
                for (Map.Entry<String, String> assayEntry : def.assayValues.entrySet()) {
                    if (!geneAssays.containsKey(assayEntry.getKey())) {
                        geneAssays.put(assayEntry.getKey(), assayEntry.getValue());
                    }
                }
            }

            List<AlleleTyperResult.AssayDefinition> assayDefs = new ArrayList<>();
            for (Map.Entry<String, String> assayEntry : geneAssays.entrySet()) {
                assayDefs.add(new AlleleTyperResult.AssayDefinition(assayEntry.getKey(), assayEntry.getValue()));
            }

            translationList.add(new AlleleTyperResult.TranslationInfo(gene, assayDefs));
        }

        return translationList;
    }

    // -----------------------------------------------------------------------
    // Diplotype calling — core algorithm
    // -----------------------------------------------------------------------

    /**
     * Call diplotype for a single gene by testing all pairs of haplotypes.
     * Returns all compatible pairs — each represents one possible diplotype assignment.
     */
    List<DiplotypePair> callDiplotype(String gene, List<HaplotypeDefinition> haplotypes,
                                      Set<String> relevantAssays, Map<String, String> observedGenotypes,
                                      Map<String, Integer> cnvData) {
        // Separate SNP assays from CNV assays
        Set<String> snpAssays = new LinkedHashSet<>();
        Set<String> cnvAssays = new LinkedHashSet<>();
        for (String assay : relevantAssays) {
            if (cnvColumnNames.contains(assay)) {
                cnvAssays.add(assay);
            } else {
                snpAssays.add(assay);
            }
        }

        // Test all pairs (i, j) where i <= j
        List<DiplotypePair> compatiblePairs = new ArrayList<>();
        for (int i = 0; i < haplotypes.size(); i++) {
            for (int j = i; j < haplotypes.size(); j++) {
                HaplotypeDefinition h1 = haplotypes.get(i);
                HaplotypeDefinition h2 = haplotypes.get(j);

                if (isPairCompatible(h1, h2, snpAssays, cnvAssays, observedGenotypes, cnvData)) {
                    compatiblePairs.add(new DiplotypePair(h1, h2));
                }
            }
        }

        return compatiblePairs;
    }

    /**
     * Check if a pair of haplotypes is compatible with observed genotypes.
     */
    boolean isPairCompatible(HaplotypeDefinition h1, HaplotypeDefinition h2,
                             Set<String> snpAssays, Set<String> cnvAssays,
                             Map<String, String> observedGenotypes,
                             Map<String, Integer> cnvData) {
        boolean geneHasCnvAssays = !cnvAssays.isEmpty();

        // Check SNP assays
        for (String assay : snpAssays) {
            String observed = observedGenotypes.get(assay);
            String exp1 = h1.assayValues.getOrDefault(assay, "");
            String exp2 = h2.assayValues.getOrDefault(assay, "");

            if (!isAssayCompatible(observed, exp1, exp2, geneHasCnvAssays)) {
                return false;
            }
        }

        // Check CNV constraints if CNV data is available
        if (!cnvAssays.isEmpty() && !cnvData.isEmpty()) {
            if (!checkCnvConstraints(h1, h2, cnvAssays, cnvData)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if a single observed assay value is compatible with expected values from two haplotypes.
     *
     * Rules:
     * - UND or null observed → true (wildcard, compatible with anything)
     * - noamp observed → both expected must be noamp or empty
     * - Homozygous X/X → both X/X, or hemizygous (one X + one noamp)
     * - Heterozygous X/Y → one haplotype provides X, the other provides Y
     * - Empty expected → wildcard (compatible with anything on that side)
     *
     * @param geneHasCnvAssays if true, noamp observed is treated strictly (both expected must be noamp/empty);
     *                          if false, noamp is treated as wildcard (like UND)
     */
    boolean isAssayCompatible(String observed, String exp1, String exp2, boolean geneHasCnvAssays) {
        // UND or null/empty observed → wildcard, always compatible
        if (observed == null || observed.isEmpty() || "UND".equals(observed)) {
            return true;
        }

        // noamp observed: for genes WITH CNV assays (e.g., CYP2D6), noamp has biological meaning
        // (gene deletion), so both expected must be noamp or empty.
        // For genes WITHOUT CNV assays, noamp is a technical failure, treated as wildcard.
        if ("noamp".equalsIgnoreCase(observed)) {
            if (geneHasCnvAssays) {
                boolean e1Ok = exp1.isEmpty() || "noamp".equalsIgnoreCase(exp1);
                boolean e2Ok = exp2.isEmpty() || "noamp".equalsIgnoreCase(exp2);
                return e1Ok && e2Ok;
            }
            return true;
        }

        // Both expected empty → wildcard, compatible with any observed
        if (exp1.isEmpty() && exp2.isEmpty()) {
            return true;
        }

        // One expected empty → wildcard on that side
        if (exp1.isEmpty()) {
            // exp2 must match at least one allele of observed
            return wildcardMatchesSide(observed, exp2);
        }
        if (exp2.isEmpty()) {
            // exp1 must match at least one allele of observed
            return wildcardMatchesSide(observed, exp1);
        }

        // Both expected are noamp → observed should be noamp, but we already handled noamp above
        if ("noamp".equalsIgnoreCase(exp1) && "noamp".equalsIgnoreCase(exp2)) {
            return false; // observed is not noamp at this point
        }

        // Parse observed alleles
        String[] observedAlleles = observed.split("/");

        if (observedAlleles.length == 2) {
            String obsA = observedAlleles[0].trim();
            String obsB = observedAlleles[1].trim();

            // Homozygous: X/X
            if (obsA.equals(obsB)) {
                // Case 1: both expected = X → homozygous match
                if (alleleEquals(exp1, obsA) && alleleEquals(exp2, obsA)) {
                    return true;
                }
                // Case 2: hemizygous — one is X, one is noamp
                if (alleleEquals(exp1, obsA) && "noamp".equalsIgnoreCase(exp2)) {
                    return true;
                }
                if ("noamp".equalsIgnoreCase(exp1) && alleleEquals(exp2, obsA)) {
                    return true;
                }
                return false;
            }

            // Heterozygous: X/Y
            // exp1=X, exp2=Y or exp1=Y, exp2=X
            if ((alleleEquals(exp1, obsA) && alleleEquals(exp2, obsB))
                    || (alleleEquals(exp1, obsB) && alleleEquals(exp2, obsA))) {
                return true;
            }

            // Hemizygous heterozygous: one expected is noamp
            // noamp + X should show as X/X (homozygous), so heterozygous with noamp doesn't make sense
            // But allow: one noamp and one matches one of the observed alleles
            // Actually for heterozygous, noamp scenario is unlikely. Return false.
            return false;
        }

        // Single allele observed (no slash)
        // This shouldn't normally happen in diploid data, but handle gracefully
        if (alleleEquals(exp1, observed) && alleleEquals(exp2, observed)) {
            return true;
        }
        if (alleleEquals(exp1, observed) && "noamp".equalsIgnoreCase(exp2)) {
            return true;
        }
        if ("noamp".equalsIgnoreCase(exp1) && alleleEquals(exp2, observed)) {
            return true;
        }

        return false;
    }

    /**
     * When one side of the expected pair is empty (wildcard), check if the defined side
     * matches at least one allele of the observed call.
     */
    private boolean wildcardMatchesSide(String observed, String defined) {
        if ("noamp".equalsIgnoreCase(defined)) {
            // noamp on defined side + wildcard on other side:
            // observed should show something consistent.
            // If observed is a normal genotype like X/X or X/Y, the noamp contributes nothing,
            // so the other (wildcard) side must account for all observed alleles.
            // We allow it since the wildcard side is unconstrained.
            return true;
        }

        String[] observedAlleles = observed.split("/");
        for (String obs : observedAlleles) {
            if (alleleEquals(defined, obs.trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compare expected allele value with observed allele, handling special cases.
     */
    private boolean alleleEquals(String expected, String observed) {
        if (expected == null || observed == null) {
            return false;
        }
        if (expected.equalsIgnoreCase(observed)) {
            return true;
        }
        // Handle noamp explicitly
        if ("noamp".equalsIgnoreCase(expected) || "noamp".equalsIgnoreCase(observed)) {
            return false;
        }
        return false;
    }

    /**
     * Check CNV constraints for a haplotype pair.
     * Sum h1's CN value + h2's CN value for each CNV column.
     * Must equal observed CN Predicted. For ">=N" values, use minimum-bound check.
     */
    boolean checkCnvConstraints(HaplotypeDefinition h1, HaplotypeDefinition h2,
                                Set<String> cnvAssays, Map<String, Integer> cnvData) {
        for (String cnvAssay : cnvAssays) {
            String exp1 = h1.assayValues.getOrDefault(cnvAssay, "");
            String exp2 = h2.assayValues.getOrDefault(cnvAssay, "");

            // Both empty → no constraint for this CNV
            if (exp1.isEmpty() && exp2.isEmpty()) {
                continue;
            }

            // Find observed CN value. The cnvData map uses target names, but we need to
            // match against the assay/target. Try matching the CNV assay directly.
            Integer observedCN = cnvData.get(cnvAssay);
            if (observedCN == null) {
                // CNV data not available for this assay — skip constraint
                continue;
            }

            // Parse expected CN values
            int minSum = 0;
            boolean hasGeConstraint = false;

            if (StringUtils.isNotEmpty(exp1)) {
                if (exp1.startsWith(">=")) {
                    hasGeConstraint = true;
                    minSum += parseIntOrZero(exp1.substring(2));
                } else {
                    minSum += parseIntOrZero(exp1);
                }
            }

            if (StringUtils.isNotEmpty(exp2)) {
                if (exp2.startsWith(">=")) {
                    hasGeConstraint = true;
                    minSum += parseIntOrZero(exp2.substring(2));
                } else {
                    minSum += parseIntOrZero(exp2);
                }
            }

            if (hasGeConstraint) {
                if (observedCN < minSum) {
                    return false;
                }
            } else {
                if (observedCN != minSum) {
                    return false;
                }
            }
        }

        return true;
    }

    private int parseIntOrZero(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    // -----------------------------------------------------------------------
    // Result formatting
    // -----------------------------------------------------------------------

    /**
     * Format diplotype result based on the number of compatible pairs.
     *
     * Scenarios:
     * 1. 0 pairs → "no translation available"
     * 2. 1 pair → "allele1/allele2"
     * 3. Multiple pairs, ≤2 distinct alleles → "{A/A, A/B, B/B}" (curly brackets)
     * 4. All assays UND/noamp → "no translation available" (handled before calling this)
     * 5. Multiple pairs, >2 distinct alleles → "d1, d2, d3" (no curly brackets)
     */
    DiplotypeCallResult formatDiplotypeResult(List<DiplotypePair> compatiblePairs) {
        if (compatiblePairs.isEmpty()) {
            return new DiplotypeCallResult("no translation available", Collections.emptyList());
        }

        // Collect distinct allele names and diplotype strings
        Set<String> distinctAlleles = new TreeSet<>();
        List<String> diplotypeStrings = new ArrayList<>();

        for (DiplotypePair pair : compatiblePairs) {
            distinctAlleles.add(pair.h1.allele);
            distinctAlleles.add(pair.h2.allele);
            diplotypeStrings.add(pair.toString());
        }

        if (diplotypeStrings.size() == 1) {
            // Scenario 1: single pair
            return new DiplotypeCallResult(diplotypeStrings.get(0), new ArrayList<>(distinctAlleles));
        }

        if (distinctAlleles.size() <= 2) {
            // Scenario 2/3: multiple pairs but ≤2 distinct alleles → curly brackets
            String diplotype = "{" + String.join(", ", diplotypeStrings) + "}";
            return new DiplotypeCallResult(diplotype, new ArrayList<>(distinctAlleles));
        }

        // Scenario 5: multiple pairs, >2 distinct alleles → no curly brackets
        String diplotype = String.join(", ", diplotypeStrings);
        return new DiplotypeCallResult(diplotype, new ArrayList<>(distinctAlleles));
    }

    // -----------------------------------------------------------------------
    // JSON export (preserved from original)
    // -----------------------------------------------------------------------

    public String exportToJson(List<AlleleTyperResult> results) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        return mapper.writeValueAsString(results);
    }

    public String exportToJson(AlleleTyperResult result) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        return mapper.writeValueAsString(result);
    }

    public void exportToJsonLines(List<AlleleTyperResult> results, Path outputFile) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();

        try (java.io.BufferedWriter writer = new java.io.BufferedWriter(new java.io.FileWriter(outputFile.toFile()))) {
            for (AlleleTyperResult result : results) {
                String json = mapper.writeValueAsString(result);
                writer.write(json);
                writer.newLine();
            }
        }

        logger.info("Exported {} samples to {}", results.size(), outputFile);
    }

    // -----------------------------------------------------------------------
    // Inner classes
    // -----------------------------------------------------------------------

    /**
     * Haplotype definition: a star allele with its expected assay values.
     */
    static class HaplotypeDefinition {
        final String gene;
        final String allele;
        final Map<String, String> assayValues; // assayId -> expected value (includes CNV columns)

        HaplotypeDefinition(String gene, String allele, Map<String, String> assayValues) {
            this.gene = gene;
            this.allele = allele;
            this.assayValues = assayValues;
        }
    }

    /**
     * A pair of haplotypes forming a diplotype.
     */
    static class DiplotypePair {
        final HaplotypeDefinition h1;
        final HaplotypeDefinition h2;

        DiplotypePair(HaplotypeDefinition h1, HaplotypeDefinition h2) {
            this.h1 = h1;
            this.h2 = h2;
        }

        @Override
        public String toString() {
            // Alphabetical ordering for consistency
            String a1 = h1.allele;
            String a2 = h2.allele;
            if (a1.compareTo(a2) <= 0) {
                return a1 + "/" + a2;
            }
            return a2 + "/" + a1;
        }
    }

    /**
     * Result of diplotype calling for a single gene.
     */
    static class DiplotypeCallResult {
        final String diplotype;
        final List<String> distinctAlleles;

        DiplotypeCallResult(String diplotype, List<String> distinctAlleles) {
            this.diplotype = diplotype;
            this.distinctAlleles = distinctAlleles;
        }
    }
}
