package org.opencb.opencga.analysis.clinical.pharmacogenomics;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.StarAlleleAnnotation;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class AlleleTyperTest {

    private Path genotypingFile;
    private Path translationFile;
    private Path expectedResultsFile;
    private Map<String, Map<String, String>> expectedResults;

//    @Before
//    public void setUp() throws IOException {
//        // Input files
//        genotypingFile = Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/3G_Export_DO_TrueMark_128_Export_DO_TrueMark_128_Genotyping_07-11-2025-074600.txt");
//        translationFile = Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/PGX_SNP_CNV_128_OA_translation_RevC.csv");
//
//        // Expected results file
//        expectedResultsFile = Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/TrueMark128_detail_result.csv");
//
//        // Parse expected results
//        expectedResults = parseExpectedResults();
//    }

    /**
     * Parse the expected results from ThermoFisher output.
     */
//    private Map<String, Map<String, String>> parseExpectedResults() throws IOException {
//        Map<String, Map<String, String>> results = new HashMap<>();
//
//        try (BufferedReader br = new BufferedReader(new FileReader(expectedResultsFile.toFile()))) {
//            String headerLine = br.readLine(); // Skip first header line with rsIDs
//            String dataHeaderLine = br.readLine(); // Second line with gene names
//
//            if (dataHeaderLine == null) {
//                return results;
//            }
//
//            String[] headers = dataHeaderLine.split(",", -1);
//            // Headers: sample ID, UGT2B17, CYP2D6, CYP2C9, CYP2C19, CYP2B6, TPMT, SLCO1B1, NUDT15, ...
//
//            String line;
//            while ((line = br.readLine()) != null) {
//                String[] fields = line.split(",", -1);
//                if (fields.length < 2) {
//                    continue;
//                }
//
//                String sampleId = fields[0].trim();
//                if ("NTC".equals(sampleId) || sampleId.isEmpty()) {
//                    continue; // Skip NTC (No Template Control)
//                }
//
//                Map<String, String> geneResults = new HashMap<>();
//                for (int i = 1; i < Math.min(fields.length, headers.length); i++) {
//                    String gene = headers[i].trim();
//                    String result = fields[i].trim();
//                    // Include all non-empty gene names, even if result is empty or "no translation available"
//                    if (!gene.isEmpty() && !gene.equals("Notes")) {
//                        geneResults.put(gene, result);
//                    }
//                }
//
//                results.put(sampleId, geneResults);
//            }
//        }
//
//        return results;
//    }

//    @Test
//    public void testParseTranslationFile() throws IOException {
//        AlleleTyper typer = new AlleleTyper();
//        typer.parseTranslationFile(translationFile);
//
//        // Verify that translation file was parsed successfully
//        // We should have definitions for multiple genes
//        assertNotNull(typer);
//    }

//    @Test
//    public void testParseGenotypingFile() throws IOException {
//        AlleleTyper typer = new AlleleTyper();
//        typer.parseTranslationFile(translationFile);
//
//        Map<String, AlleleTyper.PharmacogenomicsProfile> results = typer.parseGenotypingFileAndCallAlleles(genotypingFile);
//
//        // Verify we got results for all samples
//        assertNotNull(results);
//        assertTrue("Should have parsed multiple samples", results.size() > 0);
//
//        // Print results for debugging
//        System.out.println("\n=== AlleleTyper Results ===");
//        for (Map.Entry<String, AlleleTyper.PharmacogenomicsProfile> entry : results.entrySet()) {
//            System.out.println(entry.getValue());
//        }
//    }

//    @Test
//    public void testCYP2D6Calling() throws IOException {
//        AlleleTyper typer = new AlleleTyper();
//        typer.parseTranslationFile(translationFile);
//
//        Map<String, AlleleTyper.PharmacogenomicsProfile> results = typer.parseGenotypingFileAndCallAlleles(genotypingFile);
//
//        compareGeneResults("CYP2D6", results, true);
//    }

//    @Test
//    public void testComprehensiveComparison() throws IOException {
//        AlleleTyper typer = new AlleleTyper();
//        typer.parseTranslationFile(translationFile);
//
//        Map<String, AlleleTyper.PharmacogenomicsProfile> results = typer.parseGenotypingFileAndCallAlleles(genotypingFile);
//
//        // Get list of all genes from expected results
//        Set<String> allGenes = new java.util.HashSet<>();
//        for (Map<String, String> geneResults : expectedResults.values()) {
//            allGenes.addAll(geneResults.keySet());
//        }
//
//        System.out.println("\n=== COMPREHENSIVE COMPARISON WITH OFFICIAL RESULTS ===\n");
//
//        // Compare each gene
//        for (String gene : allGenes) {
//            if (gene.equals("Notes") || gene.contains("_cn")) {
//                continue; // Skip notes and CNV columns
//            }
//            compareGeneResults(gene, results, false);
//        }
//    }

//    private void compareGeneResults(String geneName, Map<String, AlleleTyper.PharmacogenomicsProfile> results, boolean failOnNoMatches) {
//        System.out.println("\n=== " + geneName + " Results Comparison ===");
//        System.out.println(String.format("%-15s %-45s %-45s %s", "Sample", "Expected", "Obtained", "Match"));
//        System.out.println("-".repeat(130));
//
//        int totalSamples = 0;
//        int exactMatches = 0;
//        int partialMatches = 0;
//        int noTranslationExpected = 0;
//        int noTranslationObtained = 0;
//        int bothNoTranslation = 0;
//
//        for (Map.Entry<String, AlleleTyper.PharmacogenomicsProfile> entry : results.entrySet()) {
//            String sampleId = entry.getKey();
//            AlleleTyper.PharmacogenomicsProfile profile = entry.getValue();
//
//            if (!expectedResults.containsKey(sampleId)) {
//                continue;
//            }
//
//            totalSamples++;
//            String expected = expectedResults.get(sampleId).get(geneName);
//            List<String> obtainedAlleles = profile.getGeneAlleles().get(geneName);
//
//            // Handle expected result
//            boolean expectedNoTranslation = expected == null || expected.isEmpty() ||
//                                           expected.equals("no translation available");
//
//            // Handle obtained result
//            boolean obtainedNoResult = obtainedAlleles == null || obtainedAlleles.isEmpty();
//
//            String obtainedStr = obtainedNoResult ? "[]" : obtainedAlleles.toString();
//            String expectedStr = expectedNoTranslation ? "no translation available" : expected;
//
//            // Determine match status
//            String matchStatus = "MISS";
//            boolean isMatch = false;
//
//            if (expectedNoTranslation && obtainedNoResult) {
//                matchStatus = "CORRECT (no translation)";
//                bothNoTranslation++;
//                isMatch = true;
//            } else if (expectedNoTranslation) {
//                matchStatus = "FALSE POSITIVE";
//                noTranslationExpected++;
//            } else if (obtainedNoResult) {
//                matchStatus = "FALSE NEGATIVE";
//                noTranslationObtained++;
//            } else {
//                // Both have results - check for matches
//                boolean exact = false;
//                boolean partial = false;
//
//                for (String allele : obtainedAlleles) {
//                    if (expected.contains(allele)) {
//                        partial = true;
//                        // Check if it's an exact match (considering the format {*1/*5} vs *1/*5)
//                        String cleanExpected = expected.replaceAll("[{}]", "").trim();
//                        if (cleanExpected.equals(allele) || cleanExpected.equals(obtainedStr.replaceAll("[\\[\\]]", ""))) {
//                            exact = true;
//                        }
//                        break;
//                    }
//                }
//
//                if (exact) {
//                    matchStatus = "EXACT";
//                    exactMatches++;
//                    partialMatches++;
//                    isMatch = true;
//                } else if (partial) {
//                    matchStatus = "PARTIAL";
//                    partialMatches++;
//                    isMatch = true;
//                }
//            }
//
//            System.out.println(String.format("%-15s %-45s %-45s %s",
//                sampleId,
//                truncate(expectedStr, 45),
//                truncate(obtainedStr, 45),
//                matchStatus
//            ));
//        }
//
//        System.out.println("-".repeat(130));
//        System.out.println(String.format("Total samples: %d", totalSamples));
//        System.out.println(String.format("Exact matches: %d (%.1f%%)", exactMatches, 100.0 * exactMatches / totalSamples));
//        System.out.println(String.format("Partial matches: %d (%.1f%%)", partialMatches, 100.0 * partialMatches / totalSamples));
//        System.out.println(String.format("Both 'no translation': %d (%.1f%%)", bothNoTranslation, 100.0 * bothNoTranslation / totalSamples));
//        System.out.println(String.format("False positives: %d (%.1f%%)", noTranslationExpected, 100.0 * noTranslationExpected / totalSamples));
//        System.out.println(String.format("False negatives: %d (%.1f%%)", noTranslationObtained, 100.0 * noTranslationObtained / totalSamples));
//
//        int totalCorrect = exactMatches + bothNoTranslation;
//        System.out.println(String.format("Overall accuracy: %d/%d (%.1f%%)", totalCorrect, totalSamples, 100.0 * totalCorrect / totalSamples));
//        System.out.println();
//
//        if (failOnNoMatches) {
//            assertTrue("Should have at least some matching alleles for " + geneName, partialMatches > 0 || bothNoTranslation > 0);
//        }
//    }

//    @Test
//    public void testGenerateCompleteJsonLines() throws IOException {
//        AlleleTyper typer = new AlleleTyper();
//        typer.parseTranslationFile(translationFile);
//
//        // Build comprehensive results for all samples
//        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);
//
//        // Export to JSONL file
//        Path outputFile = Paths.get("/home/imedina/appl/opencga/pharmacogenomics_results.jsonl");
//        typer.exportToJsonLines(results, outputFile);
//
//        System.out.println("\n=== Complete Results Generated ===");
//        System.out.println("Output file: " + outputFile);
//        System.out.println("Total samples: " + results.size());
//        System.out.println("Format: JSONL (one JSON per line)");
//
//        // Print summary statistics
//        int samplesWithResults = 0;
//        int totalGenes = 0;
//        for (AlleleTyperResult result : results) {
//            if (!result.getStarAlleles().isEmpty()) {
//                samplesWithResults++;
//                totalGenes += result.getStarAlleles().size();
//            }
//        }
//
//        System.out.println("Samples with star allele calls: " + samplesWithResults);
//        System.out.println("Average genes per sample: " + (samplesWithResults > 0 ? (double)totalGenes / samplesWithResults : 0));
//
//        assertTrue("Output file should exist", outputFile.toFile().exists());
//        assertTrue("Should have results for all samples", results.size() > 0);
//    }

//    @Test
//    public void testJsonOutput() throws IOException {
//        AlleleTyper typer = new AlleleTyper();
//        typer.parseTranslationFile(translationFile);
//
//        // Build comprehensive results
//        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);
//
//        assertNotNull("Should have results", results);
//        assertTrue("Should have multiple samples", results.size() > 0);
//
//        // Export first sample to JSON
//        if (!results.isEmpty()) {
//            AlleleTyperResult firstSample = results.get(0);
//            String json = typer.exportToJson(firstSample);
//
//            System.out.println("\n=== Sample JSON Output ===");
//            System.out.println(json);
//            System.out.println("\n=== JSON Output for " + firstSample.getSampleId() + " ===");
//
//            // Verify structure
//            assertNotNull("Should have sample ID", firstSample.getSampleId());
//            assertNotNull("Should have star alleles", firstSample.getStarAlleles());
//            assertNotNull("Should have genotypes", firstSample.getGenotypes());
//            assertNotNull("Should have translation info", firstSample.getTranslation());
//
//            System.out.println("Sample: " + firstSample.getSampleId());
//            System.out.println("Star alleles found: " + firstSample.getStarAlleles().size() + " genes");
//            System.out.println("Total genotypes: " + firstSample.getGenotypes().size());
//            System.out.println("Translation info for: " + firstSample.getTranslation().size() + " genes");
//        }
//    }

//    @Test
//    public void testSpecificSamples() throws IOException {
//        AlleleTyper typer = new AlleleTyper();
//        typer.parseTranslationFile(translationFile);
//
//        Map<String, AlleleTyper.PharmacogenomicsProfile> results = typer.parseGenotypingFileAndCallAlleles(genotypingFile);
//
//        // Test NA14476 - Expected: {*2/*41, *21/*41, *29/*41}
//        AlleleTyper.PharmacogenomicsProfile na14476 = results.get("NA14476");
//        assertNotNull("Should have results for NA14476", na14476);
//
//        List<String> cyp2d6Alleles = na14476.getGeneAlleles().get("CYP2D6");
//        assertNotNull("Should have CYP2D6 alleles for NA14476", cyp2d6Alleles);
//
//        System.out.println("\nNA14476 CYP2D6 alleles found: " + cyp2d6Alleles);
//        System.out.println("Expected one of: {*2/*41, *21/*41, *29/*41}");
//
//        // Check if we found at least *2, *21, *29, or *41
//        boolean foundRelevantAllele = false;
//        for (String allele : cyp2d6Alleles) {
//            if (allele.contains("*2") || allele.contains("*21") || allele.contains("*29") || allele.contains("*41")) {
//                foundRelevantAllele = true;
//                break;
//            }
//        }
//
//        assertTrue("Should find at least one relevant allele (*2, *21, *29, or *41)", foundRelevantAllele);
//
//        // Test NA17114 - Expected: *1/*5 (simpler case)
//        AlleleTyper.PharmacogenomicsProfile na17114 = results.get("NA17114");
//        assertNotNull("Should have results for NA17114", na17114);
//
//        List<String> cyp2d6Alleles2 = na17114.getGeneAlleles().get("CYP2D6");
//        System.out.println("\nNA17114 CYP2D6 alleles found: " + cyp2d6Alleles2);
//        System.out.println("Expected: *1/*5");
//    }

    @Test
    @Category(MediumTests.class)
    public void testGenerateAnnotatedJsonLines() throws IOException {
        Path genotypingFile = Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/3G_Export_DO_TrueMark_128_Export_DO_TrueMark_128_Genotyping_07-11-2025-074600.txt");
        Path translationFile = Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/PGX_SNP_CNV_128_OA_translation_RevC.csv");

        // 1. Run AlleleTyper
        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);
        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);
        System.out.println("AlleleTyper produced " + results.size() + " sample results");

        // 2. Annotate with CellBase pharmacogenomics
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .setVersion("v6.7")
                .setDefaultSpecies("hsapiens")
                .setRest(new RestConfig(Collections.singletonList("https://ws.zettagenomics.com/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient(clientConfiguration);
        StarAlleleAnnotator annotator = new StarAlleleAnnotator(cellBaseClient);

        for (AlleleTyperResult result : results) {
            if (result == null || result.getAlleleTyperResults() == null) {
                continue;
            }
            for (AlleleTyperResult.StarAlleleResult starAlleleResult : result.getAlleleTyperResults()) {
                String gene = starAlleleResult.getGene();
                if (gene == null || gene.isEmpty() || starAlleleResult.getAlleleCalls() == null) {
                    continue;
                }
                for (AlleleTyperResult.AlleleCall alleleCall : starAlleleResult.getAlleleCalls()) {
                    StarAlleleAnnotation annotation = annotator.annotate(gene, alleleCall.getAllele());
                    alleleCall.setAnnotation(annotation);
                    System.out.println("  Annotated " + gene + " " + alleleCall.getAllele()
                            + " -> " + annotation.getDrugs().size() + " drugs");
                }
            }
        }

        // 3. Export to JSONL
        Path outputFile = Paths.get("/home/imedina/projects/SESPA/pgx/pharmacogenomics_results.jsonl");
        typer.exportToJsonLines(results, outputFile);

        System.out.println("\nOutput file: " + outputFile);
        System.out.println("Total samples: " + results.size());
        assertTrue("Output file should exist", outputFile.toFile().exists());
    }

    private String truncate(String str, int maxLength) {
        if (str == null || str.length() <= maxLength) {
            return str;
        }
        return str.substring(0, maxLength - 3) + "...";
    }
}
