package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;

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
import java.util.*;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class AlleleTyperTest {

    private static final String BASE_DIR = "/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Pharmacogenetics";
    private static final String FQU71_BASE_DIR = "/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/FQU71";

    private Path genotypingFile;
    private Path translationFile;
    private Path cnvFile;
    private Path expectedResultsFile;
    private Map<String, Map<String, String>> expectedResults;

    @Before
    public void setUp() throws IOException {
        genotypingFile = Paths.get(BASE_DIR, "FQV18_DO_online_export_FQV18_DO_online_export_Genotyping_26-02-2026-114007.txt");
        translationFile = Paths.get(BASE_DIR, "PGX_SNP_CNV_128_OA_translation_RevC tab.txt");
        cnvFile = Paths.get(BASE_DIR, "FQV18_DO_online_export_FQV18_DO_online_export_Copy_Number_Variation_Result_multi_plate_26-02-2026-114007.txt");
        expectedResultsFile = Paths.get(BASE_DIR, "FQV18_20260226_detail.csv");

        expectedResults = parseExpectedResults();
    }

    private Map<String, Map<String, String>> parseExpectedResults() throws IOException {
        Map<String, Map<String, String>> results = new LinkedHashMap<>();

        if (!expectedResultsFile.toFile().exists()) {
            return results;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(expectedResultsFile.toFile()))) {
            // Find the header line that starts with "sample ID"
            List<String> headers = null;
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("sample ID")) {
                    headers = parseCsvLine(line);
                    break;
                }
            }

            if (headers == null) {
                return results;
            }

            while ((line = br.readLine()) != null) {
                List<String> fields = parseCsvLine(line);
                if (fields.size() < 2) {
                    continue;
                }

                String sampleId = fields.get(0).trim();
                if ("NTC".equals(sampleId) || sampleId.isEmpty()) {
                    continue;
                }

                Map<String, String> geneResults = new LinkedHashMap<>();
                for (int i = 1; i < Math.min(fields.size(), headers.size()); i++) {
                    String gene = headers.get(i).trim();
                    String result = fields.get(i).trim();
                    if (!gene.isEmpty() && !"Notes".equals(gene)) {
                        geneResults.put(gene, result);
                    }
                }

                results.put(sampleId, geneResults);
            }
        }

        return results;
    }

    /**
     * Parse a CSV line handling quoted fields (fields containing commas wrapped in double quotes).
     */
    private List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder current = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());

        return fields;
    }

    @Test
    public void testParseTranslationFile() throws IOException {
        if (!translationFile.toFile().exists()) {
            System.out.println("Skipping test: translation file not found at " + translationFile);
            return;
        }

        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        assertNotNull(typer);
    }

    @Test
    public void testAlleleTyperWithoutCnv() throws IOException {
        if (!genotypingFile.toFile().exists() || !translationFile.toFile().exists()) {
            System.out.println("Skipping test: input files not found");
            return;
        }

        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);

        assertNotNull(results);
        assertTrue("Should have parsed multiple samples", results.size() > 0);

        System.out.println("\n=== AlleleTyper Results (without CNV) ===");
        for (AlleleTyperResult result : results) {
            System.out.println("Sample: " + result.getSampleId());
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                System.out.println("  " + star.getGene() + ": " + star.getDiplotype());
            }
        }
    }

    @Test
    public void testAlleleTyperWithCnvAndExport() throws IOException {
        if (!genotypingFile.toFile().exists() || !translationFile.toFile().exists()) {
            System.out.println("Skipping test: input files not found");
            return;
        }

        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        if (cnvFile.toFile().exists()) {
            typer.parseCnvFile(cnvFile);
        }

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);

        assertNotNull(results);
        assertTrue("Should have parsed multiple samples", results.size() > 0);

        // Export JSON results to /tmp
        Path jsonOutputFile = Paths.get("/tmp/pharmacogenomics_allele_typer_results.json");
        typer.exportToJsonLines(results, jsonOutputFile);
        System.out.println("JSON results written to: " + jsonOutputFile);

        // Export CSV summary to /tmp
        Path csvOutputFile = Paths.get("/tmp/pharmacogenomics_allele_typer_results.csv");
        exportResultsToCsv(results, csvOutputFile);
        System.out.println("CSV results written to: " + csvOutputFile);
    }

    private void exportResultsToCsv(List<AlleleTyperResult> results, Path outputFile) throws IOException {
        // Collect all gene names in order
        Set<String> allGenes = new LinkedHashSet<>();
        for (AlleleTyperResult result : results) {
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                allGenes.add(star.getGene());
            }
        }

        try (java.io.BufferedWriter writer = new java.io.BufferedWriter(new java.io.FileWriter(outputFile.toFile()))) {
            // Header
            writer.write("Sample");
            for (String gene : allGenes) {
                writer.write("," + gene);
            }
            writer.newLine();

            // Data: join multiple diplotypes per gene with " | "
            for (AlleleTyperResult result : results) {
                Map<String, List<String>> geneMap = new LinkedHashMap<>();
                for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                    geneMap.computeIfAbsent(star.getGene(), k -> new ArrayList<>())
                           .add(star.getDiplotype() != null ? star.getDiplotype() : "");
                }

                writer.write(result.getSampleId());
                for (String gene : allGenes) {
                    writer.write("," + joinDiplotypes(geneMap.get(gene)));
                }
                writer.newLine();
            }
        }
    }

    /**
     * Join a list of diplotype strings into a single comparable string.
     * A single diplotype is returned as-is; multiple are wrapped in curly brackets.
     */
    private String joinDiplotypes(List<String> diplotypes) {
        if (diplotypes == null || diplotypes.isEmpty()) {
            return "";
        }
        if (diplotypes.size() == 1) {
            return diplotypes.get(0);
        }
        return "{" + String.join(", ", diplotypes) + "}";
    }

    @Test
    public void testAlleleTyperWithCnv() throws IOException {
        if (!genotypingFile.toFile().exists() || !translationFile.toFile().exists()) {
            System.out.println("Skipping test: input files not found");
            return;
        }

        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        if (cnvFile.toFile().exists()) {
            typer.parseCnvFile(cnvFile);
        }

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);

        assertNotNull(results);
        assertTrue("Should have parsed multiple samples", results.size() > 0);

        System.out.println("\n=== AlleleTyper Results (with CNV) ===");
        for (AlleleTyperResult result : results) {
            System.out.println("Sample: " + result.getSampleId());
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                System.out.println("  " + star.getGene() + ": " + star.getDiplotype());
            }
        }
    }

    @Test
    public void testComprehensiveComparison() throws IOException {
        if (!genotypingFile.toFile().exists() || !translationFile.toFile().exists()
                || !expectedResultsFile.toFile().exists()) {
            System.out.println("Skipping test: input/expected files not found");
            return;
        }

        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        if (cnvFile.toFile().exists()) {
            typer.parseCnvFile(cnvFile);
        }

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);

        // Build lookup: sampleId -> gene -> list of diplotypes (one per compatible pair)
        Map<String, Map<String, List<String>>> obtainedResults = new LinkedHashMap<>();
        for (AlleleTyperResult result : results) {
            Map<String, List<String>> geneMap = new LinkedHashMap<>();
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                geneMap.computeIfAbsent(star.getGene(), k -> new ArrayList<>()).add(star.getDiplotype());
            }
            obtainedResults.put(result.getSampleId(), geneMap);
        }

        // Get genes that exist in both obtained results and expected results
        Set<String> obtainedGenes = new LinkedHashSet<>();
        for (Map<String, List<String>> geneMap : obtainedResults.values()) {
            obtainedGenes.addAll(geneMap.keySet());
        }

        System.out.println("\n=== COMPREHENSIVE COMPARISON WITH OFFICIAL RESULTS ===\n");

        for (String gene : obtainedGenes) {
            compareGeneResults(gene, obtainedResults);
        }
    }

    @Test
    public void testSpecificSamples() throws IOException {
        if (!genotypingFile.toFile().exists() || !translationFile.toFile().exists()) {
            System.out.println("Skipping test: input files not found");
            return;
        }

        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        if (cnvFile.toFile().exists()) {
            typer.parseCnvFile(cnvFile);
        }

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);

        Map<String, Map<String, List<String>>> obtainedResults = new LinkedHashMap<>();
        for (AlleleTyperResult result : results) {
            Map<String, List<String>> geneMap = new LinkedHashMap<>();
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                geneMap.computeIfAbsent(star.getGene(), k -> new ArrayList<>()).add(star.getDiplotype());
            }
            obtainedResults.put(result.getSampleId(), geneMap);
        }

        // Check specific known samples
        System.out.println("\n=== Specific Sample Checks ===");

        checkSampleGene(obtainedResults, "MN04", "CYP2D6", "*2/*35");
        checkSampleGene(obtainedResults, "1900113", "CYP2D6", "*1/*4");
    }

    private void checkSampleGene(Map<String, Map<String, List<String>>> obtainedResults,
                                  String sampleId, String gene, String expected) {
        Map<String, List<String>> sampleResults = obtainedResults.get(sampleId);
        if (sampleResults == null) {
            System.out.println(sampleId + " " + gene + ": SAMPLE NOT FOUND");
            return;
        }

        List<String> obtainedList = sampleResults.get(gene);
        String obtained = joinDiplotypes(obtainedList);
        boolean match = normalizedMatch(expected, obtained);
        System.out.println(String.format("%-15s %-10s: expected=%-25s obtained=%-25s %s",
                sampleId, gene, expected, obtained, match ? "MATCH" : "MISMATCH"));
    }

    private void compareGeneResults(String geneName, Map<String, Map<String, List<String>>> obtainedResults) {
        System.out.println("\n=== " + geneName + " Results Comparison ===");
        System.out.println(String.format("%-15s %-45s %-45s %s", "Sample", "Expected", "Obtained", "Match"));
        System.out.println(new String(new char[130]).replace('\0', '-'));

        int totalSamples = 0;
        int exactMatches = 0;
        int bothNoTranslation = 0;

        for (Map.Entry<String, Map<String, String>> expectedEntry : expectedResults.entrySet()) {
            String sampleId = expectedEntry.getKey();
            String expected = expectedEntry.getValue().get(geneName);

            Map<String, List<String>> sampleObtained = obtainedResults.get(sampleId);
            if (sampleObtained == null) {
                continue;
            }

            totalSamples++;
            String obtained = joinDiplotypes(sampleObtained.get(geneName));

            boolean expectedNoTranslation = expected == null || expected.isEmpty()
                    || "no translation available".equals(expected);
            boolean obtainedNoTranslation = obtained == null || obtained.isEmpty()
                    || "no translation available".equals(obtained);

            String expectedStr = expectedNoTranslation ? "no translation available" : expected;
            String obtainedStr = obtainedNoTranslation ? "no translation available" : obtained;

            String matchStatus;
            if (expectedNoTranslation && obtainedNoTranslation) {
                matchStatus = "CORRECT (no translation)";
                bothNoTranslation++;
            } else if (expectedStr.equals(obtainedStr)) {
                matchStatus = "EXACT";
                exactMatches++;
            } else if (!expectedNoTranslation && !obtainedNoTranslation && normalizedMatch(expectedStr, obtainedStr)) {
                matchStatus = "MATCH (normalized)";
                exactMatches++;
            } else {
                matchStatus = "MISMATCH";
            }

            System.out.println(String.format("%-15s %-45s %-45s %s",
                    sampleId,
                    truncate(expectedStr, 45),
                    truncate(obtainedStr, 45),
                    matchStatus));
        }

        if (totalSamples > 0) {
            System.out.println(new String(new char[130]).replace('\0', '-'));
            int totalCorrect = exactMatches + bothNoTranslation;
            System.out.println(String.format("Total: %d, Exact matches: %d, No translation: %d, Accuracy: %.1f%%",
                    totalSamples, exactMatches, bothNoTranslation, 100.0 * totalCorrect / totalSamples));
        }
        System.out.println();
    }

    /**
     * Normalized match: compare after sorting alleles within each diplotype string.
     * E.g., "{*1/*4, *1/*5}" matches "{*1/*5, *1/*4}"
     */
    private boolean normalizedMatch(String expected, String obtained) {
        return normalize(expected).equals(normalize(obtained));
    }

    private String normalize(String diplotype) {
        if (diplotype == null) {
            return "";
        }
        // Remove curly brackets
        String clean = diplotype.replaceAll("[{}]", "").trim();
        // Split by comma
        String[] parts = clean.split(",");
        List<String> normalized = new ArrayList<>();
        for (String part : parts) {
            // Normalize each diplotype: sort the two alleles
            String trimmed = part.trim();
            String[] alleles = trimmed.split("/");
            if (alleles.length == 2) {
                Arrays.sort(alleles);
                normalized.add(alleles[0] + "/" + alleles[1]);
            } else {
                normalized.add(trimmed);
            }
        }
        Collections.sort(normalized);
        return normalized.toString();
    }

    @Test
    @Category(MediumTests.class)
    public void testCpicAnnotationAndExport() throws IOException {
        if (!genotypingFile.toFile().exists() || !translationFile.toFile().exists()) {
            System.out.println("Skipping test: input files not found");
            return;
        }

        // 1. Run AlleleTyper
        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        if (cnvFile.toFile().exists()) {
            typer.parseCnvFile(cnvFile);
        }

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(genotypingFile);
        System.out.println("AlleleTyper produced " + results.size() + " sample results");

        // 2. Annotate with CellBase + CPIC
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .setVersion("v6.7")
                .setDefaultSpecies("hsapiens")
                .setRest(new RestConfig(Collections.singletonList("https://ws.zettagenomics.com/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient(clientConfiguration);
        PharmacogenomicsManager manager = new PharmacogenomicsManager(null);
        manager.annotateResults(results, cellBaseClient);

        // 3. Print summary of CPIC annotations
        int annotatedGenes = 0;
        int totalRecommendations = 0;
        for (AlleleTyperResult result : results) {
            if (result.getAlleleTyperResults() == null) {
                continue;
            }
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                if (star.getDiplotypeAnnotation() != null) {
                    annotatedGenes++;
                    int drugCount = star.getDiplotypeAnnotation().getDrugs() != null
                            ? star.getDiplotypeAnnotation().getDrugs().size() : 0;
                    totalRecommendations += drugCount;
                    System.out.println("  " + result.getSampleId() + " " + star.getGene()
                            + " " + star.getDiplotype()
                            + " -> phenotype=" + (star.getDiplotypeAnnotation().getDiplotypeInfo() != null
                                ? star.getDiplotypeAnnotation().getDiplotypeInfo().getGeneresult() : "N/A")
                            + ", " + drugCount + " drugs");
                }
            }
        }
        System.out.println("\nCPIC annotation summary: " + annotatedGenes + " gene/sample pairs annotated, "
                + totalRecommendations + " total drugs");

        // 4. Export one JSON file per sample
        Path outputDir = Paths.get("/tmp/pgx_annotated_results");
        java.nio.file.Files.createDirectories(outputDir);
        com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
        objectMapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        long totalSize = 0;
        for (AlleleTyperResult r : results) {
            byte[] json = objectMapper.writeValueAsBytes(r);
            Path samplePath = outputDir.resolve(r.getSampleId() + ".json");
            java.nio.file.Files.write(samplePath, json);
            totalSize += json.length;
        }
        System.out.println("\nAnnotated results written to: " + outputDir + " (" + results.size() + " files)");
        System.out.println("Total serialized size: " + String.format("%.2f", totalSize / 1024.0) + " KB");
    }

    // -----------------------------------------------------------------------
    // FQU71 experiment tests
    // -----------------------------------------------------------------------

    @Test
    public void testFQU71AlleleTyperWithCnv() throws IOException {
        Path fqu71Genotyping = Paths.get(FQU71_BASE_DIR,
                "20260318_FQU71_DO_20260318_FQU71_DO_Genotyping_18-03-2026-122836.txt");
        Path fqu71Cnv = Paths.get(FQU71_BASE_DIR,
                "20260318_FQU71_DO_20260318_FQU71_DO_Copy_Number_Variation_Result_multi_plate_18-03-2026-122836.txt");

        if (!fqu71Genotyping.toFile().exists() || !translationFile.toFile().exists()) {
            System.out.println("Skipping test: FQU71 input files not found");
            return;
        }

        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        if (fqu71Cnv.toFile().exists()) {
            typer.parseCnvFile(fqu71Cnv);
        }

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(fqu71Genotyping);

        assertNotNull(results);
        assertTrue("Should have parsed multiple samples", results.size() > 0);

        System.out.println("\n=== FQU71 AlleleTyper Results (with CNV) ===");
        for (AlleleTyperResult result : results) {
            System.out.println("Sample: " + result.getSampleId());
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                System.out.println("  " + star.getGene() + ": " + star.getDiplotype());
            }
        }

        // Export CSV and JSON
        Path csvOutputFile = Paths.get("/tmp/fqu71_allele_typer_results.csv");
        exportResultsToCsv(results, csvOutputFile);
        System.out.println("\nCSV results written to: " + csvOutputFile);

        Path jsonOutputFile = Paths.get("/tmp/fqu71_allele_typer_results.json");
        typer.exportToJsonLines(results, jsonOutputFile);
        System.out.println("JSON results written to: " + jsonOutputFile);
    }

    @Test
    public void testFQU71ComprehensiveComparison() throws IOException {
        Path fqu71Genotyping = Paths.get(FQU71_BASE_DIR,
                "20260318_FQU71_DO_20260318_FQU71_DO_Genotyping_18-03-2026-122836.txt");
        Path fqu71Cnv = Paths.get(FQU71_BASE_DIR,
                "20260318_FQU71_DO_20260318_FQU71_DO_Copy_Number_Variation_Result_multi_plate_18-03-2026-122836.txt");
        Path fqu71DetailedResults = Paths.get(FQU71_BASE_DIR, "FQU71_20260318_detailed.txt");

        if (!fqu71Genotyping.toFile().exists() || !translationFile.toFile().exists()
                || !fqu71DetailedResults.toFile().exists()) {
            System.out.println("Skipping test: FQU71 input/expected files not found");
            return;
        }

        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        if (fqu71Cnv.toFile().exists()) {
            typer.parseCnvFile(fqu71Cnv);
        }

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(fqu71Genotyping);

        // Build lookup: sampleId -> gene -> list of diplotypes
        Map<String, Map<String, List<String>>> obtainedResults = new LinkedHashMap<>();
        for (AlleleTyperResult result : results) {
            Map<String, List<String>> geneMap = new LinkedHashMap<>();
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                geneMap.computeIfAbsent(star.getGene(), k -> new ArrayList<>()).add(star.getDiplotype());
            }
            obtainedResults.put(result.getSampleId(), geneMap);
        }

        // Parse TrueMark detailed results (tab-separated format)
        Map<String, Map<String, String>> fqu71Expected = parseDetailedTsvResults(fqu71DetailedResults);

        // Get genes from obtained results
        Set<String> obtainedGenes = new LinkedHashSet<>();
        for (Map<String, List<String>> geneMap : obtainedResults.values()) {
            obtainedGenes.addAll(geneMap.keySet());
        }

        // Write comparison report to file and stdout
        Path reportFile = Paths.get("/tmp/fqu71_comparison_report.txt");
        writeComparisonReport(obtainedGenes, obtainedResults, fqu71Expected, reportFile);
        System.out.println("\nComparison report written to: " + reportFile);
    }

    @Test
    @Category(MediumTests.class)
    public void testFQU71CpicAnnotationAndExport() throws IOException {
        Path fqu71Genotyping = Paths.get(FQU71_BASE_DIR,
                "20260318_FQU71_DO_20260318_FQU71_DO_Genotyping_18-03-2026-122836.txt");
        Path fqu71Cnv = Paths.get(FQU71_BASE_DIR,
                "20260318_FQU71_DO_20260318_FQU71_DO_Copy_Number_Variation_Result_multi_plate_18-03-2026-122836.txt");

        if (!fqu71Genotyping.toFile().exists() || !translationFile.toFile().exists()) {
            System.out.println("Skipping test: FQU71 input files not found");
            return;
        }

        // 1. Run AlleleTyper
        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFile(translationFile);

        if (fqu71Cnv.toFile().exists()) {
            typer.parseCnvFile(fqu71Cnv);
        }

        List<AlleleTyperResult> results = typer.buildAlleleTyperResults(fqu71Genotyping);
        System.out.println("FQU71 AlleleTyper produced " + results.size() + " sample results");

        // 2. Annotate with CellBase + CPIC
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .setVersion("v6.7")
                .setDefaultSpecies("hsapiens")
                .setRest(new RestConfig(Collections.singletonList("https://ws.zettagenomics.com/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient(clientConfiguration);
        PharmacogenomicsManager manager = new PharmacogenomicsManager(null);
        manager.annotateResults(results, cellBaseClient);

        // 3. Print summary of CPIC annotations
        int annotatedGenes = 0;
        int totalRecommendations = 0;
        for (AlleleTyperResult result : results) {
            if (result.getAlleleTyperResults() == null) {
                continue;
            }
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                if (star.getDiplotypeAnnotation() != null) {
                    annotatedGenes++;
                    int drugCount = star.getDiplotypeAnnotation().getDrugs() != null
                            ? star.getDiplotypeAnnotation().getDrugs().size() : 0;
                    totalRecommendations += drugCount;
                    System.out.println("  " + result.getSampleId() + " " + star.getGene()
                            + " " + star.getDiplotype()
                            + " -> phenotype=" + (star.getDiplotypeAnnotation().getDiplotypeInfo() != null
                                ? star.getDiplotypeAnnotation().getDiplotypeInfo().getGeneresult() : "N/A")
                            + ", " + drugCount + " drugs");
                }
            }
        }
        System.out.println("\nFQU71 CPIC annotation summary: " + annotatedGenes + " gene/sample pairs annotated, "
                + totalRecommendations + " total drugs");

        // 4. Export one JSON file per sample
        Path outputDir = Paths.get("/tmp/fqu71_pgx_annotated_results");
        java.nio.file.Files.createDirectories(outputDir);
        com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
        objectMapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        long totalSize = 0;
        for (AlleleTyperResult r : results) {
            byte[] json = objectMapper.writeValueAsBytes(r);
            Path samplePath = outputDir.resolve(r.getSampleId() + ".json");
            java.nio.file.Files.write(samplePath, json);
            totalSize += json.length;
        }
        System.out.println("\nFQU71 annotated results written to: " + outputDir + " (" + results.size() + " files)");
        System.out.println("Total serialized size: " + String.format("%.2f", totalSize / 1024.0) + " KB");
    }

    /**
     * Parse the TrueMark detailed results file (tab-separated format).
     * The file may use \r (CR only) as line separators (classic Mac format).
     * The header line contains rsIDs and column names. Gene columns appear after "sample ID"
     * and before "Notes". Data lines follow with sample results.
     */
    private Map<String, Map<String, String>> parseDetailedTsvResults(Path detailedFile) throws IOException {
        Map<String, Map<String, String>> results = new LinkedHashMap<>();

        // Read entire file and split by \r or \n or \r\n to handle all line ending formats
        String content = new String(java.nio.file.Files.readAllBytes(detailedFile));
        String[] lines = content.split("\\r\\n|\\r|\\n");

        if (lines.length < 2) {
            System.out.println("WARNING: detailed results file has fewer than 2 lines");
            return results;
        }

        // The header line contains "sample ID" — find it (may not be the first line)
        String headerLine = null;
        int headerLineIndex = -1;
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].contains("sample ID")) {
                headerLine = lines[i];
                headerLineIndex = i;
                break;
            }
        }

        if (headerLine == null) {
            System.out.println("WARNING: 'sample ID' column not found in detailed results");
            return results;
        }

        String[] headers = headerLine.split("\t", -1);

        // Find "sample ID" column and gene columns (between "sample ID" and "Notes")
        int sampleIdIndex = -1;
        int notesIndex = -1;
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim();
            if ("sample ID".equalsIgnoreCase(h)) {
                sampleIdIndex = i;
            } else if ("Notes".equalsIgnoreCase(h) && sampleIdIndex >= 0) {
                notesIndex = i;
                break;
            }
        }

        if (sampleIdIndex < 0) {
            System.out.println("WARNING: 'sample ID' index not found after scanning headers");
            return results;
        }
        if (notesIndex < 0) {
            notesIndex = headers.length;
        }

        // Gene columns are between sampleIdIndex+1 and notesIndex-1
        List<String> geneNames = new ArrayList<>();
        for (int i = sampleIdIndex + 1; i < notesIndex; i++) {
            geneNames.add(headers[i].trim());
        }

        System.out.println("Detailed results gene columns (" + geneNames.size() + "): " + geneNames);

        // Parse data lines (everything after the header line)
        for (int lineIdx = headerLineIndex + 1; lineIdx < lines.length; lineIdx++) {
            String line = lines[lineIdx];
            if (line.trim().isEmpty()) {
                continue;
            }
            String[] fields = line.split("\t", -1);
            if (fields.length <= sampleIdIndex) {
                continue;
            }

            String sampleId = fields[sampleIdIndex].trim();
            if (sampleId.isEmpty() || "NTC".equals(sampleId)) {
                continue;
            }

            // Skip control samples (CALT, CALTBIS, CHET, CHETBIS, CREF)
            if (sampleId.startsWith("CALT") || sampleId.startsWith("CHET") || sampleId.startsWith("CREF")) {
                continue;
            }

            Map<String, String> geneResults = new LinkedHashMap<>();
            for (int i = 0; i < geneNames.size(); i++) {
                int colIndex = sampleIdIndex + 1 + i;
                if (colIndex < fields.length) {
                    String value = fields[colIndex].trim();
                    geneResults.put(geneNames.get(i), value);
                }
            }

            results.put(sampleId, geneResults);
        }

        System.out.println("Parsed " + results.size() + " samples from detailed results");
        return results;
    }

    /**
     * Write comparison report to file and stdout, matching the format from pharmacogenomics_comparison_report.txt.
     */
    private void writeComparisonReport(Set<String> genes,
                                        Map<String, Map<String, List<String>>> obtainedResults,
                                        Map<String, Map<String, String>> expectedResults,
                                        Path reportFile) throws IOException {
        try (java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.FileWriter(reportFile.toFile()))) {
            String separator = new String(new char[130]).replace('\0', '-');

            pw.println("=== COMPREHENSIVE COMPARISON WITH OFFICIAL RESULTS ===");
            pw.println();

            int totalGenes = 0;
            int totalMatches = 0;
            int totalMismatches = 0;
            int totalNoTranslation = 0;

            for (String gene : genes) {
                pw.println();
                pw.println("=== " + gene + " Results Comparison ===");
                pw.println(String.format("%-15s %-45s %-45s %s", "Sample", "Expected", "Obtained", "Match"));
                pw.println(separator);

                int geneMatches = 0;
                int geneMismatches = 0;
                int geneNoTranslation = 0;
                int geneSamples = 0;

                for (Map.Entry<String, Map<String, String>> expectedEntry : expectedResults.entrySet()) {
                    String sampleId = expectedEntry.getKey();
                    String expected = expectedEntry.getValue().get(gene);

                    Map<String, List<String>> sampleObtained = obtainedResults.get(sampleId);
                    if (sampleObtained == null) {
                        continue;
                    }

                    geneSamples++;
                    String obtained = joinDiplotypes(sampleObtained.get(gene));

                    boolean expectedNoTranslation = expected == null || expected.isEmpty()
                            || "no translation available".equals(expected);
                    boolean obtainedNoTranslation = obtained == null || obtained.isEmpty()
                            || "no translation available".equals(obtained);

                    String expectedStr = expectedNoTranslation ? "no translation available" : expected;
                    String obtainedStr = obtainedNoTranslation ? "no translation available" : obtained;

                    String matchStatus;
                    if (expectedNoTranslation && obtainedNoTranslation) {
                        matchStatus = "CORRECT (no translation)";
                        geneNoTranslation++;
                    } else if (expectedStr.equals(obtainedStr)) {
                        matchStatus = "EXACT";
                        geneMatches++;
                    } else if (!expectedNoTranslation && !obtainedNoTranslation
                            && normalizedMatch(expectedStr, obtainedStr)) {
                        matchStatus = "MATCH (normalized)";
                        geneMatches++;
                    } else {
                        matchStatus = "MISMATCH";
                        geneMismatches++;
                    }

                    String line = String.format("%-15s %-45s %-45s %s",
                            sampleId,
                            truncate(expectedStr, 45),
                            truncate(obtainedStr, 45),
                            matchStatus);
                    pw.println(line);
                }

                if (geneSamples > 0) {
                    pw.println(separator);
                    int geneTotal = geneMatches + geneNoTranslation;
                    pw.println(String.format("Total: %d, Exact matches: %d, No translation: %d, Accuracy: %.1f%%",
                            geneSamples, geneMatches, geneNoTranslation, 100.0 * geneTotal / geneSamples));
                }
                pw.println();

                totalGenes++;
                totalMatches += geneMatches;
                totalMismatches += geneMismatches;
                totalNoTranslation += geneNoTranslation;
            }

            pw.println();
            pw.println("=== OVERALL SUMMARY ===");
            int overallTotal = totalMatches + totalNoTranslation + totalMismatches;
            int overallCorrect = totalMatches + totalNoTranslation;
            pw.println(String.format("Genes: %d, Total comparisons: %d, Matches: %d, No translation: %d, Mismatches: %d, Accuracy: %.1f%%",
                    totalGenes, overallTotal, totalMatches, totalNoTranslation, totalMismatches,
                    overallTotal > 0 ? 100.0 * overallCorrect / overallTotal : 0.0));
        }

        // Also print to stdout
        System.out.println(new String(java.nio.file.Files.readAllBytes(reportFile)));
    }

    private String truncate(String str, int maxLength) {
        if (str == null || str.length() <= maxLength) {
            return str;
        }
        return str.substring(0, maxLength - 3) + "...";
    }
}
