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

import org.opencb.opencga.core.models.clinical.AlleleTyperResult;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * AlleleTyper processes pharmacogenomics genotyping data and calls star alleles
 * based on genotype patterns defined in a translation file.
 */
public class AlleleTyper {

    private Logger logger = LoggerFactory.getLogger(AlleleTyper.class);

    // Parsed translation data
    private Map<String, List<StarAlleleDefinition>> geneAlleleDefinitions;
    private Map<String, String> assayToRsIdMap;
    private Map<String, Integer> assayToColumnMap; // Maps assay ID to column index in translation file

    public AlleleTyper() {
        this.geneAlleleDefinitions = new HashMap<>();
        this.assayToRsIdMap = new HashMap<>();
        this.assayToColumnMap = new HashMap<>();
    }

    /**
     * Parse the translation file containing star allele definitions from String content.
     *
     * @param translationContent Translation file content as String
     * @throws IOException if content cannot be parsed
     */
    public void parseTranslationFromString(String translationContent) throws IOException {
        logger.info("Parsing translation content from String");

        try (BufferedReader br = new BufferedReader(new java.io.StringReader(translationContent))) {
            parseTranslationFromReader(br);
        }
    }

    /**
     * Parse the translation file containing star allele definitions.
     *
     * @param translationFile Path to the translation CSV file
     * @throws IOException if file cannot be read
     */
    public void parseTranslationFile(Path translationFile) throws IOException {
        logger.info("Parsing translation file: {}", translationFile);

        try (BufferedReader br = new BufferedReader(new FileReader(translationFile.toFile()))) {
            parseTranslationFromReader(br);
        }
    }

    /**
     * Parse translation data from a BufferedReader.
     * Common implementation for both file and string parsing.
     */
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

                // Line 2: rsIDs header (starts at column 5, after gene, allele, 3 CNV columns)
                if (lineNumber == 2) {
                    rsIds = fields;
                    continue;
                }

                // Line 3: Assay IDs and column headers (gene, allele, CNV columns, then assay IDs)
                if (lineNumber == 3) {
                    assayIds = fields;
                    // Build assay to rsID mapping and column index mapping
                    buildAssayMaps(assayIds, rsIds);
                    continue;
                }

                // Line 4+: Star allele definitions
                if (fields.length >= 3) {
                    parseStarAlleleDefinition(fields, assayIds);
                }
            }

        logger.info("Parsed {} genes with star allele definitions", geneAlleleDefinitions.size());
    }

    /**
     * Parse genotyping output file and call star alleles for all samples.
     *
     * @param genotypingFile Path to the genotyping output file
     * @return Map of sample ID to their pharmacogenomics profile
     * @throws IOException if file cannot be read
     */
    public Map<String, PharmacogenomicsProfile> parseGenotypingFileAndCallAlleles(Path genotypingFile) throws IOException {
        logger.info("Parsing genotyping file: {}", genotypingFile);

        // First pass: collect all genotypes per sample
        Map<String, Map<String, String>> sampleGenotypes = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(genotypingFile.toFile()))) {
            String line;
            boolean headerParsed = false;
            int sampleIdIndex = -1;
            int assayNameIndex = -1;
            int callIndex = -1;
            int geneIndex = -1;
            int rsIdIndex = -1;

            while ((line = br.readLine()) != null) {
                // Skip comment lines and empty lines
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }

                String[] fields = line.split("\t", -1);

                // Parse header
                if (!headerParsed) {
                    for (int i = 0; i < fields.length; i++) {
                        String header = fields[i].trim();
                        if ("Sample ID".equals(header)) {
                            sampleIdIndex = i;
                        } else if ("Assay Name".equals(header)) {
                            assayNameIndex = i;
                        } else if ("Call".equals(header)) {
                            callIndex = i;
                        } else if ("Gene Symbol".equals(header)) {
                            geneIndex = i;
                        } else if ("NCBI SNP Reference".equals(header)) {
                            rsIdIndex = i;
                        }
                    }
                    headerParsed = true;
                    logger.debug("Header parsed: sampleIdIndex={}, assayNameIndex={}, callIndex={}",
                                 sampleIdIndex, assayNameIndex, callIndex);
                    continue;
                }

                // Parse data lines
                if (sampleIdIndex >= 0 && assayNameIndex >= 0 && callIndex >= 0) {
                    String sampleId = fields[sampleIdIndex].trim();
                    String assayName = fields[assayNameIndex].trim();
                    String call = fields[callIndex].trim();

                    if (StringUtils.isNotEmpty(sampleId) && StringUtils.isNotEmpty(assayName)) {
                        sampleGenotypes.computeIfAbsent(sampleId, k -> new HashMap<>()).put(assayName, call);
                    }
                }
            }
        }

        logger.info("Parsed genotypes for {} samples", sampleGenotypes.size());

        // Second pass: call star alleles for each sample
        Map<String, PharmacogenomicsProfile> results = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : sampleGenotypes.entrySet()) {
            String sampleId = entry.getKey();
            Map<String, String> genotypes = entry.getValue();
            PharmacogenomicsProfile profile = callStarAlleles(sampleId, genotypes);
            results.put(sampleId, profile);
        }

        return results;
    }

    /**
     * Call star alleles for a single sample based on their genotypes.
     *
     * @param sampleId Sample identifier
     * @param genotypes Map of assay name to genotype call
     * @return Pharmacogenomics profile with called alleles
     */
    private PharmacogenomicsProfile callStarAlleles(String sampleId, Map<String, String> genotypes) {
        PharmacogenomicsProfile profile = new PharmacogenomicsProfile(sampleId);

        // Debug for first sample
        boolean firstSample = sampleGenotypesDebugPrinted == false;
        if (firstSample) {
            sampleGenotypesDebugPrinted = true;
            System.out.println("DEBUG: Sample " + sampleId + " has " + genotypes.size() + " genotypes");
            int count = 0;
            for (String key : genotypes.keySet()) {
                System.out.println("  Sample genotype key: " + key);
                if (++count >= 5) break;
            }
        }

        // For each gene, find matching star alleles
        for (Map.Entry<String, List<StarAlleleDefinition>> geneEntry : geneAlleleDefinitions.entrySet()) {
            String gene = geneEntry.getKey();
            List<StarAlleleDefinition> alleleDefinitions = geneEntry.getValue();

            List<String> matchedAlleles = new ArrayList<>();

            // Check each star allele definition against sample genotypes
            for (StarAlleleDefinition definition : alleleDefinitions) {
                // Debug first allele of first gene
                if (firstSample && gene.equals("CYP2D6") && definition.getAllele().equals("*1")) {
                    System.out.println("DEBUG: Checking CYP2D6 *1 with " + definition.getGenotypes().size() + " genotypes");
                    int count = 0;
                    for (String key : definition.getGenotypes().keySet()) {
                        System.out.println("  Definition genotype key: " + key);
                        if (++count >= 5) break;
                    }
                }

                boolean matches = matchesAllele(definition, genotypes);
                if (matches) {
                    matchedAlleles.add(definition.getAllele());
                    if (firstSample && gene.equals("CYP2D6")) {
                        System.out.println("DEBUG: CYP2D6 " + definition.getAllele() + " MATCHED!");
                    }
                }
            }

            if (!matchedAlleles.isEmpty()) {
                profile.addGeneAlleles(gene, matchedAlleles);
                if (firstSample && gene.equals("CYP2D6")) {
                    System.out.println("DEBUG: Adding CYP2D6 to profile with " + matchedAlleles.size() + " alleles: " + matchedAlleles);
                }
            } else if (firstSample && gene.equals("CYP2D6")) {
                System.out.println("DEBUG: NO CYP2D6 alleles matched for " + sampleId);
            }
        }

        return profile;
    }

    /**
     * Build comprehensive AlleleTyperResult from String content.
     *
     * @param genotypingContent Genotyping file content as String
     * @return List of AlleleTyperResult objects, one per sample
     * @throws IOException if content cannot be parsed
     */
    public List<AlleleTyperResult> buildAlleleTyperResultsFromString(String genotypingContent) throws IOException {
        try (BufferedReader br = new BufferedReader(new java.io.StringReader(genotypingContent))) {
            return buildAlleleTyperResultsFromReader(br);
        }
    }

    /**
     * Build comprehensive AlleleTyperResult from profiles and genotypes.
     *
     * @param genotypingFile Path to genotyping file
     * @return List of AlleleTyperResult objects, one per sample
     * @throws IOException if file cannot be read
     */
    public List<AlleleTyperResult> buildAlleleTyperResults(Path genotypingFile) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(genotypingFile.toFile()))) {
            return buildAlleleTyperResultsFromReader(br);
        }
    }

    /**
     * Build AlleleTyperResult from a BufferedReader.
     * Common implementation for both file and string parsing.
     */
    private List<AlleleTyperResult> buildAlleleTyperResultsFromReader(BufferedReader br) throws IOException {
        // Parse genotypes and collect sample data
        Map<String, Map<String, String>> sampleGenotypes = new HashMap<>();
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

                if (sampleIdIndex >= 0 && assayNameIndex >= 0 && callIndex >= 0) {
                    String sampleId = fields[sampleIdIndex].trim();
                    String assayName = fields[assayNameIndex].trim();
                    String call = fields[callIndex].trim();

                    if (StringUtils.isNotEmpty(sampleId) && StringUtils.isNotEmpty(assayName)) {
                        sampleGenotypes.computeIfAbsent(sampleId, k -> new HashMap<>()).put(assayName, call);
                    }
                }
            }

        // Build results for each sample
        List<AlleleTyperResult> results = new ArrayList<>();

        for (Map.Entry<String, Map<String, String>> entry : sampleGenotypes.entrySet()) {
            String sampleId = entry.getKey();
            Map<String, String> genotypes = entry.getValue();

            // Call star alleles
            PharmacogenomicsProfile profile = callStarAlleles(sampleId, genotypes);

            // Build AlleleTyperResult
            AlleleTyperResult result = buildSampleResult(sampleId, profile, genotypes);
            results.add(result);
        }

        return results;
    }

    /**
     * Build AlleleTyperResult for a single sample.
     */
    private AlleleTyperResult buildSampleResult(String sampleId, PharmacogenomicsProfile profile,
                                                      Map<String, String> genotypes) {
        // Build star allele results
        List<AlleleTyperResult.StarAlleleResult> starAlleles = new ArrayList<>();

        for (Map.Entry<String, List<String>> geneEntry : profile.getGeneAlleles().entrySet()) {
            String gene = geneEntry.getKey();
            List<String> alleles = geneEntry.getValue();

            // Get variants used for this gene
            List<String> geneVariants = getVariantsForGene(gene);

            // Build allele calls
            List<AlleleTyperResult.AlleleCall> alleleCalls = new ArrayList<>();
            for (String allele : alleles) {
                alleleCalls.add(new AlleleTyperResult.AlleleCall(allele));
            }

            starAlleles.add(new AlleleTyperResult.StarAlleleResult(gene, alleleCalls, geneVariants));
        }

        // Build genotype list
        List<AlleleTyperResult.Genotype> genotypeList = new ArrayList<>();
        for (Map.Entry<String, String> genoEntry : genotypes.entrySet()) {
            genotypeList.add(new AlleleTyperResult.Genotype(genoEntry.getKey(), genoEntry.getValue()));
        }

        // Build translation info
        List<AlleleTyperResult.TranslationInfo> translationList = buildTranslationInfo();

        return new AlleleTyperResult(sampleId, starAlleles, genotypeList, translationList);
    }

    /**
     * Get list of variants (assay IDs) used for a specific gene.
     */
    private List<String> getVariantsForGene(String gene) {
        Set<String> variants = new HashSet<>();
        List<StarAlleleDefinition> definitions = geneAlleleDefinitions.get(gene);

        if (definitions != null) {
            for (StarAlleleDefinition def : definitions) {
                variants.addAll(def.getGenotypes().keySet());
            }
        }

        return new ArrayList<>(variants);
    }

    /**
     * Build translation information for all genes.
     */
    private List<AlleleTyperResult.TranslationInfo> buildTranslationInfo() {
        List<AlleleTyperResult.TranslationInfo> translationList = new ArrayList<>();

        for (Map.Entry<String, List<StarAlleleDefinition>> entry : geneAlleleDefinitions.entrySet()) {
            String gene = entry.getKey();

            // Get unique assays for this gene
            Map<String, String> geneAssays = new HashMap<>();
            for (StarAlleleDefinition def : entry.getValue()) {
                for (Map.Entry<String, String> genoEntry : def.getGenotypes().entrySet()) {
                    if (!geneAssays.containsKey(genoEntry.getKey())) {
                        geneAssays.put(genoEntry.getKey(), genoEntry.getValue());
                    }
                }
            }

            // Build assay definitions
            List<AlleleTyperResult.AssayDefinition> assayDefs = new ArrayList<>();
            for (Map.Entry<String, String> assayEntry : geneAssays.entrySet()) {
                assayDefs.add(new AlleleTyperResult.AssayDefinition(
                    assayEntry.getKey(),
                    assayEntry.getValue()
                ));
            }

            translationList.add(new AlleleTyperResult.TranslationInfo(gene, assayDefs));
        }

        return translationList;
    }

    /**
     * Export AlleleTyperResults to JSON string.
     *
     * @param results List of results to export
     * @return JSON string
     * @throws IOException if JSON serialization fails
     */
    public String exportToJson(List<AlleleTyperResult> results) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        return mapper.writeValueAsString(results);
    }

    /**
     * Export a single AlleleTyperResult to JSON string.
     *
     * @param result Result to export
     * @return JSON string
     * @throws IOException if JSON serialization fails
     */
    public String exportToJson(AlleleTyperResult result) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        return mapper.writeValueAsString(result);
    }

    /**
     * Export results to JSONL file (one JSON per line).
     *
     * @param results List of results to export
     * @param outputFile Path to output file
     * @throws IOException if file cannot be written
     */
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

    private boolean sampleGenotypesDebugPrinted = false;

    /**
     * Analyze which assays are missing for failed samples of a specific gene.
     */
    public void analyzeMissingAssays(String geneName, List<String> failedSampleIds,
                                      Map<String, PharmacogenomicsProfile> allResults) {
        // Get all assays used by this gene from the translation file
        Set<String> geneAssays = new HashSet<>();
        List<StarAlleleDefinition> geneAlleles = geneAlleleDefinitions.get(geneName);

        if (geneAlleles == null || geneAlleles.isEmpty()) {
            System.out.println("No allele definitions found for " + geneName);
            return;
        }

        // Collect all assays used by any allele of this gene
        for (StarAlleleDefinition definition : geneAlleles) {
            geneAssays.addAll(definition.getGenotypes().keySet());
        }

        System.out.println(geneName + " uses " + geneAssays.size() + " assays");

        // Analyze first few failed samples
        int samplesToAnalyze = Math.min(3, failedSampleIds.size());
        System.out.println("Analyzing first " + samplesToAnalyze + " failed samples:\n");

        // We need access to the original sample genotypes - let's re-parse for analysis
        try {
            Map<String, Map<String, String>> sampleGenotypes = parseSampleGenotypes(
                java.nio.file.Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/3G_Export_DO_TrueMark_128_Export_DO_TrueMark_128_Genotyping_07-11-2025-074600.txt")
            );

            for (int i = 0; i < samplesToAnalyze; i++) {
                String sampleId = failedSampleIds.get(i);
                Map<String, String> genotypes = sampleGenotypes.get(sampleId);

                if (genotypes == null) {
                    System.out.println(sampleId + ": No genotype data found");
                    continue;
                }

                System.out.println("Sample: " + sampleId);

                // Check each gene-specific assay
                int missingCount = 0;
                int presentCount = 0;
                List<String> missingAssays = new ArrayList<>();

                for (String assay : geneAssays) {
                    String value = genotypes.get(assay);
                    if (value == null || value.equals("UND") || value.trim().isEmpty()) {
                        missingCount++;
                        missingAssays.add(assay);
                    } else {
                        presentCount++;
                    }
                }

                System.out.println("  Assays present: " + presentCount + "/" + geneAssays.size());
                System.out.println("  Assays missing: " + missingCount + "/" + geneAssays.size() +
                                 " (" + String.format("%.1f%%", 100.0 * missingCount / geneAssays.size()) + ")");

                if (missingCount > 0 && missingCount <= 10) {
                    System.out.println("  Missing: " + missingAssays);
                }

                // Now check why no alleles matched - show mismatches for first allele
                if (presentCount > 0 && !geneAlleles.isEmpty()) {
                    StarAlleleDefinition firstAllele = geneAlleles.get(0);
                    System.out.println("  Checking against " + geneName + " " + firstAllele.getAllele() + ":");

                    int matches = 0;
                    int mismatches = 0;
                    List<String> mismatchDetails = new ArrayList<>();

                    for (Map.Entry<String, String> entry : firstAllele.getGenotypes().entrySet()) {
                        String assay = entry.getKey();
                        String expected = entry.getValue();
                        String observed = genotypes.get(assay);

                        if (observed == null || observed.equals("UND")) {
                            continue; // Skip missing
                        }

                        if (genotypeMatches(expected, observed)) {
                            matches++;
                        } else {
                            mismatches++;
                            if (mismatchDetails.size() < 5) {
                                mismatchDetails.add(assay + ": expected=" + expected + ", observed=" + observed);
                            }
                        }
                    }

                    System.out.println("    Matches: " + matches + ", Mismatches: " + mismatches);
                    if (!mismatchDetails.isEmpty()) {
                        System.out.println("    First mismatches:");
                        for (String detail : mismatchDetails) {
                            System.out.println("      " + detail);
                        }
                    }
                }

                System.out.println();
            }
        } catch (Exception e) {
            System.out.println("Error analyzing: " + e.getMessage());
        }
    }

    /**
     * Parse genotyping file to extract sample genotypes (without calling alleles).
     */
    private Map<String, Map<String, String>> parseSampleGenotypes(java.nio.file.Path genotypingFile) throws java.io.IOException {
        Map<String, Map<String, String>> sampleGenotypes = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(genotypingFile.toFile()))) {
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

                if (sampleIdIndex >= 0 && assayNameIndex >= 0 && callIndex >= 0) {
                    String sampleId = fields[sampleIdIndex].trim();
                    String assayName = fields[assayNameIndex].trim();
                    String call = fields[callIndex].trim();

                    if (StringUtils.isNotEmpty(sampleId) && StringUtils.isNotEmpty(assayName)) {
                        sampleGenotypes.computeIfAbsent(sampleId, k -> new HashMap<>()).put(assayName, call);
                    }
                }
            }
        }

        return sampleGenotypes;
    }

    /**
     * Test a sample against all alleles of a gene to find closest matches.
     */
    public void testAgainstAllAlleles(String geneName, String sampleId, java.nio.file.Path genotypingFile) {
        try {
            Map<String, Map<String, String>> sampleGenotypes = parseSampleGenotypes(genotypingFile);
            Map<String, String> genotypes = sampleGenotypes.get(sampleId);

            if (genotypes == null) {
                System.out.println("No genotype data for " + sampleId);
                return;
            }

            List<StarAlleleDefinition> alleles = geneAlleleDefinitions.get(geneName);
            if (alleles == null) {
                System.out.println("No alleles defined for " + geneName);
                return;
            }

            System.out.println("\n=== Testing " + sampleId + " against all " + geneName + " alleles ===");

            // Test against each allele and score them
            List<AlleleScore> scores = new ArrayList<>();

            for (StarAlleleDefinition allele : alleles) {
                int total = 0;
                int matches = 0;
                int mismatches = 0;
                int missing = 0;

                for (Map.Entry<String, String> entry : allele.getGenotypes().entrySet()) {
                    String assay = entry.getKey();
                    String expected = entry.getValue();
                    String observed = genotypes.get(assay);

                    if (StringUtils.isEmpty(expected)) continue;

                    total++;

                    if (observed == null || observed.equals("UND")) {
                        missing++;
                    } else if (genotypeMatches(expected, observed)) {
                        matches++;
                    } else {
                        mismatches++;
                    }
                }

                double matchRatio = total > 0 ? (double) matches / total : 0;
                scores.add(new AlleleScore(allele.getAllele(), matches, mismatches, missing, total, matchRatio));
            }

            // Sort by match ratio descending
            scores.sort((a, b) -> Double.compare(b.matchRatio, a.matchRatio));

            // Print top 10
            System.out.println("Top 10 closest alleles:");
            for (int i = 0; i < Math.min(10, scores.size()); i++) {
                AlleleScore score = scores.get(i);
                System.out.println(String.format("  %2d. %10s: %2d/%2d matches (%.1f%%), %d mismatches, %d missing",
                    i + 1, score.allele, score.matches, score.total, score.matchRatio * 100,
                    score.mismatches, score.missing));
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static class AlleleScore {
        String allele;
        int matches;
        int mismatches;
        int missing;
        int total;
        double matchRatio;

        AlleleScore(String allele, int matches, int mismatches, int missing, int total, double matchRatio) {
            this.allele = allele;
            this.matches = matches;
            this.mismatches = mismatches;
            this.missing = missing;
            this.total = total;
            this.matchRatio = matchRatio;
        }
    }

    /**
     * Check if a sample's genotypes match a star allele definition.
     * Option 3: Allow missing data on non-critical positions.
     *
     * @param definition Star allele definition
     * @param genotypes Sample genotypes
     * @return true if all available positions match and critical positions are present
     */
    private boolean matchesAllele(StarAlleleDefinition definition, Map<String, String> genotypes) {
        // Force debug for first CYP2D6 check
        boolean debugThis = !matchDebugPrinted && definition.getGene().equals("CYP2D6") && definition.getAllele().equals("*1");
        if (debugThis) {
            System.out.println("DEBUG matchesAllele: Checking " + definition.getGene() + " " + definition.getAllele());
        }

        int totalPositions = 0;
        int matchedPositions = 0;
        int missingPositions = 0;
        int criticalMissing = 0;

        for (Map.Entry<String, String> expectedEntry : definition.getGenotypes().entrySet()) {
            String assayId = expectedEntry.getKey();
            String expectedGenotype = expectedEntry.getValue();

            // Skip empty or irrelevant positions
            if (StringUtils.isEmpty(expectedGenotype)) {
                continue;
            }

            totalPositions++;
            String observedGenotype = genotypes.get(assayId);

            if (debugThis && matchedPositions + missingPositions < 5) {
                System.out.println("    Assay " + assayId + ": expected=" + expectedGenotype + ", observed=" + observedGenotype);
            }

            // Handle missing data
            if (observedGenotype == null || "UND".equals(observedGenotype)) {
                missingPositions++;

                // Check if this is a critical position
                // Critical = actual genotype (not special markers like "-", "noamp", or CNV integers)
                boolean isCritical = isCriticalPosition(expectedGenotype);

                if (isCritical) {
                    criticalMissing++;
                    // Option 3: Allow some critical missing (up to 20%)
                    // If too many critical positions missing, it's not a match
                }
                continue;
            }

            // Match genotype
            if (genotypeMatches(expectedGenotype, observedGenotype)) {
                matchedPositions++;
            } else {
                if (debugThis) {
                    System.out.println("      MISMATCH: expected=" + expectedGenotype + " vs observed=" + observedGenotype);
                }
                return false; // Any mismatch on available data = no match
            }
        }

        if (debugThis) {
            System.out.println("  Match summary: total=" + totalPositions + ", matched=" + matchedPositions +
                             ", missing=" + missingPositions + ", criticalMissing=" + criticalMissing +
                             ", criticalMissingRatio=" + String.format("%.2f", totalPositions > 0 ? (double) criticalMissing / totalPositions : 0));
            matchDebugPrinted = true;
        }

        // Option 3 criteria:
        // 1. All available positions must match (no mismatches)
        // 2. Allow some missing data but not too much
        // 3. Critical positions should mostly be present (allow up to 20% missing)

        if (matchedPositions == 0) {
            if (debugThis) System.out.println("  REJECTED: No matched positions");
            return false; // No data matches
        }

        double criticalMissingRatio = totalPositions > 0 ? (double) criticalMissing / totalPositions : 0;

        // Allow up to 30% of positions to be missing (Option 3)
        // If this is too strict, we can increase or switch to Option 2
        if (criticalMissingRatio > 0.3) {
            if (debugThis) System.out.println("  REJECTED: Too many critical positions missing (" +
                                            String.format("%.1f%%", criticalMissingRatio * 100) + " > 30%)");
            return false;
        }

        if (debugThis) System.out.println("  ACCEPTED!");
        return true; // Match if all available positions matched and not too much missing
    }

    /**
     * Check if a position is critical (actual genotype vs special marker).
     */
    private boolean isCriticalPosition(String genotype) {
        if (genotype == null || genotype.isEmpty()) {
            return false;
        }
        // Special markers that are not critical
        if (genotype.equals("-") || genotype.equals("noamp") || genotype.equals("UND")) {
            return false;
        }
        // Single digits or simple numbers are CNV values, less critical
        if (genotype.matches("^\\d+$") || genotype.startsWith(">=")) {
            return false;
        }
        // Anything else (A/A, C/G, CTT/CTT, etc.) is critical
        return true;
    }

    private boolean matchDebugPrinted = false;

    /**
     * Check if observed genotype matches expected genotype pattern.
     *
     * @param expected Expected genotype from translation file
     * @param observed Observed genotype from sample
     * @return true if genotypes match
     */
    private boolean genotypeMatches(String expected, String observed) {
        // Exact match
        if (expected.equals(observed)) {
            return true;
        }

        // Handle single allele vs diploid notation
        // Translation file may have "T" while genotyping has "T/T" (homozygous)
        if (!expected.contains("/") && observed.contains("/")) {
            String[] observedAlleles = observed.split("/");
            if (observedAlleles.length == 2 && observedAlleles[0].equals(observedAlleles[1])) {
                // Observed is homozygous (T/T), check if it matches expected single allele (T)
                return expected.equals(observedAlleles[0]);
            }
        }

        // Reverse: expected is diploid, observed is single (less common but handle it)
        if (expected.contains("/") && !observed.contains("/")) {
            String[] expectedAlleles = expected.split("/");
            if (expectedAlleles.length == 2 && expectedAlleles[0].equals(expectedAlleles[1])) {
                return observed.equals(expectedAlleles[0]);
            }
        }

        // Handle reverse order for heterozygous calls (A/G == G/A)
        if (expected.contains("/") && observed.contains("/")) {
            String[] expectedAlleles = expected.split("/");
            String[] observedAlleles = observed.split("/");
            if (expectedAlleles.length == 2 && observedAlleles.length == 2) {
                return (expectedAlleles[0].equals(observedAlleles[0]) && expectedAlleles[1].equals(observedAlleles[1])) ||
                       (expectedAlleles[0].equals(observedAlleles[1]) && expectedAlleles[1].equals(observedAlleles[0]));
            }
        }

        // Handle CNV comparisons
        if (expected.startsWith(">=")) {
            try {
                int expectedValue = Integer.parseInt(expected.substring(2));
                int observedValue = Integer.parseInt(observed);
                return observedValue >= expectedValue;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * Build mappings: assay ID to rsID and assay ID to column index.
     */
    private void buildAssayMaps(String[] assayIds, String[] rsIds) {
        if (assayIds == null || rsIds == null) {
            return;
        }

        int minLength = Math.min(assayIds.length, rsIds.length);
        for (int i = 0; i < minLength; i++) {
            String assayId = assayIds[i].trim();
            String rsId = rsIds[i].trim();

            // Check if this is an assay column (starts with C_ or specific CNV assays)
            if (StringUtils.isNotEmpty(assayId)
                    && (assayId.startsWith("C_") || assayId.startsWith("ANGZ")
                        || assayId.startsWith("ANM") || assayId.startsWith("ANER"))) {

                // Map assay to column index
                assayToColumnMap.put(assayId, i);

                // Map assay to rsID if available
                if (StringUtils.isNotEmpty(rsId)) {
                    assayToRsIdMap.put(assayId, rsId);
                }

                logger.debug("Mapped assay {} to column {}", assayId, i);
            }
        }

        logger.info("Mapped {} assays to columns", assayToColumnMap.size());
    }

    /**
     * Parse a single star allele definition line.
     * Maps genotypes using assay IDs as keys by matching column positions.
     */
    private void parseStarAlleleDefinition(String[] fields, String[] assayIds) {
        if (fields.length < 3) {
            return;
        }

        String gene = fields[0].trim();
        String allele = fields[1].trim();

        if (StringUtils.isEmpty(gene) || StringUtils.isEmpty(allele)) {
            return;
        }

        // Parse genotypes for this allele using the assay column map
        Map<String, String> genotypes = new HashMap<>();

        for (Map.Entry<String, Integer> entry : assayToColumnMap.entrySet()) {
            String assayId = entry.getKey();
            Integer columnIndex = entry.getValue();

            // Get genotype from the corresponding column
            if (columnIndex < fields.length) {
                String genotype = fields[columnIndex].trim();
                if (StringUtils.isNotEmpty(genotype)) {
                    genotypes.put(assayId, genotype);
                }
            }
        }

        if (!genotypes.isEmpty()) {
            StarAlleleDefinition definition = new StarAlleleDefinition(gene, allele, genotypes);
            geneAlleleDefinitions.computeIfAbsent(gene, k -> new ArrayList<>()).add(definition);
        }
    }

    /**
     * Star allele definition data structure.
     */
    public static class StarAlleleDefinition {
        private String gene;
        private String allele;
        private Map<String, String> genotypes;

        public StarAlleleDefinition(String gene, String allele, Map<String, String> genotypes) {
            this.gene = gene;
            this.allele = allele;
            this.genotypes = genotypes;
        }

        public String getGene() {
            return gene;
        }

        public String getAllele() {
            return allele;
        }

        public Map<String, String> getGenotypes() {
            return genotypes;
        }
    }

    /**
     * Pharmacogenomics profile for a single sample.
     */
    public static class PharmacogenomicsProfile {
        private String sampleId;
        private Map<String, List<String>> geneAlleles;

        public PharmacogenomicsProfile(String sampleId) {
            this.sampleId = sampleId;
            this.geneAlleles = new HashMap<>();
        }

        public void addGeneAlleles(String gene, List<String> alleles) {
            this.geneAlleles.put(gene, alleles);
        }

        public String getSampleId() {
            return sampleId;
        }

        public Map<String, List<String>> getGeneAlleles() {
            return geneAlleles;
        }

        /**
         * Get diplotype string for a gene (e.g., "*1/*2").
         */
        public String getDiplotype(String gene) {
            List<String> alleles = geneAlleles.get(gene);
            if (alleles == null || alleles.isEmpty()) {
                return null;
            }
            if (alleles.size() == 1) {
                return alleles.get(0) + "/" + alleles.get(0);
            }
            if (alleles.size() == 2) {
                return alleles.get(0) + "/" + alleles.get(1);
            }
            // Multiple matches - return the most specific
            return String.join("/", alleles.subList(0, 2));
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Sample: ").append(sampleId).append("\n");
            for (Map.Entry<String, List<String>> entry : geneAlleles.entrySet()) {
                sb.append("  ").append(entry.getKey()).append(": ");
                sb.append(getDiplotype(entry.getKey()));
                sb.append(" [").append(String.join(", ", entry.getValue())).append("]");
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    // Test main method
    public static void main(String[] args) throws Exception {
        AlleleTyper typer = new AlleleTyper();

        Path translationFile = java.nio.file.Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/PGX_SNP_CNV_128_OA_translation_RevC.csv");
        Path genotypingFile = java.nio.file.Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/3G_Export_DO_TrueMark_128_Export_DO_TrueMark_128_Genotyping_07-11-2025-074600.txt");

        System.out.println("Parsing translation file...");
        typer.parseTranslationFile(translationFile);
        System.out.println("Parsed " + typer.geneAlleleDefinitions.size() + " genes");
        System.out.println("Mapped " + typer.assayToColumnMap.size() + " assays to columns");

        // Debug: print CYP2D6 allele definitions
        System.out.println("\nCYP2D6 allele definitions:");
        List<StarAlleleDefinition> cyp2d6Defs = typer.geneAlleleDefinitions.get("CYP2D6");
        if (cyp2d6Defs != null) {
            System.out.println("  Total: " + cyp2d6Defs.size() + " alleles defined");
            System.out.println("  Alleles: ");
            for (int i = 0; i < cyp2d6Defs.size(); i++) {
                System.out.print(cyp2d6Defs.get(i).getAllele());
                if (i < cyp2d6Defs.size() - 1) System.out.print(", ");
                if ((i + 1) % 10 == 0) System.out.println();
            }
            System.out.println();
        }

        System.out.println("\nParsing genotyping file...");
        Map<String, PharmacogenomicsProfile> results = typer.parseGenotypingFileAndCallAlleles(genotypingFile);
        System.out.println("Parsed " + results.size() + " samples");

        // Debug: print first sample's genotypes and check keys
        if (!results.isEmpty()) {
            Map.Entry<String, PharmacogenomicsProfile> firstEntry = results.entrySet().iterator().next();
            String firstSample = firstEntry.getKey();
            PharmacogenomicsProfile profile = firstEntry.getValue();

            System.out.println("\nFirst sample: " + firstSample);
            System.out.println("Gene alleles found: " + profile.getGeneAlleles().size());

            // Get the sample's genotype keys from the parsing
            // This requires accessing the intermediate data, so let's check the callStarAlleles method
        }

        // Print CYP2D6 results for all samples
        System.out.println("\n=== CYP2D6 Results Summary ===");
        List<String> failedSamples = new ArrayList<>();
        for (Map.Entry<String, PharmacogenomicsProfile> entry : results.entrySet()) {
            String sampleId = entry.getKey();
            PharmacogenomicsProfile profile = entry.getValue();
            List<String> cyp2d6 = profile.getGeneAlleles().get("CYP2D6");
            System.out.println(sampleId + ": " + (cyp2d6 != null ? cyp2d6 : "NO CYP2D6"));
            if (cyp2d6 == null) {
                failedSamples.add(sampleId);
            }
        }

        // Analyze failed samples
        System.out.println("\n=== Analyzing Failed Samples ===");
        typer.analyzeMissingAssays("CYP2D6", failedSamples, results);

        // Test first failed sample against ALL CYP2D6 alleles
        if (!failedSamples.isEmpty()) {
            typer.testAgainstAllAlleles("CYP2D6", failedSamples.get(0),
                java.nio.file.Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/3G_Export_DO_TrueMark_128_Export_DO_TrueMark_128_Genotyping_07-11-2025-074600.txt")
            );
        }

        // Print first few full results
        System.out.println("\n=== Sample Details (first 3) ===");
        int count = 0;
        for (Map.Entry<String, PharmacogenomicsProfile> entry : results.entrySet()) {
            System.out.println("\n" + entry.getValue());
            if (++count >= 3) break;
        }
    }
}
