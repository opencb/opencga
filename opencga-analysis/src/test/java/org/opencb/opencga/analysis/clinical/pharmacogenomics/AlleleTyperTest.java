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
import java.util.*;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class AlleleTyperTest {

    private static final String BASE_DIR = "/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Pharmacogenetics";

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

            // Data
            for (AlleleTyperResult result : results) {
                Map<String, String> geneMap = new LinkedHashMap<>();
                for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                    geneMap.put(star.getGene(), star.getDiplotype() != null ? star.getDiplotype() : "");
                }

                writer.write(result.getSampleId());
                for (String gene : allGenes) {
                    writer.write("," + geneMap.getOrDefault(gene, ""));
                }
                writer.newLine();
            }
        }
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

        // Build lookup: sampleId -> gene -> diplotype
        Map<String, Map<String, String>> obtainedResults = new LinkedHashMap<>();
        for (AlleleTyperResult result : results) {
            Map<String, String> geneMap = new LinkedHashMap<>();
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                geneMap.put(star.getGene(), star.getDiplotype());
            }
            obtainedResults.put(result.getSampleId(), geneMap);
        }

        // Get genes that exist in both obtained results and expected results
        Set<String> obtainedGenes = new LinkedHashSet<>();
        for (Map<String, String> geneMap : obtainedResults.values()) {
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

        Map<String, Map<String, String>> obtainedResults = new LinkedHashMap<>();
        for (AlleleTyperResult result : results) {
            Map<String, String> geneMap = new LinkedHashMap<>();
            for (AlleleTyperResult.StarAlleleResult star : result.getAlleleTyperResults()) {
                geneMap.put(star.getGene(), star.getDiplotype());
            }
            obtainedResults.put(result.getSampleId(), geneMap);
        }

        // Check specific known samples
        System.out.println("\n=== Specific Sample Checks ===");

        checkSampleGene(obtainedResults, "MN04", "CYP2D6", "*2/*35");
        checkSampleGene(obtainedResults, "1900113", "CYP2D6", "*1/*4");
    }

    private void checkSampleGene(Map<String, Map<String, String>> obtainedResults,
                                  String sampleId, String gene, String expected) {
        Map<String, String> sampleResults = obtainedResults.get(sampleId);
        if (sampleResults == null) {
            System.out.println(sampleId + " " + gene + ": SAMPLE NOT FOUND");
            return;
        }

        String obtained = sampleResults.get(gene);
        boolean match = expected.equals(obtained);
        System.out.println(String.format("%-15s %-10s: expected=%-25s obtained=%-25s %s",
                sampleId, gene, expected, obtained, match ? "MATCH" : "MISMATCH"));
    }

    private void compareGeneResults(String geneName, Map<String, Map<String, String>> obtainedResults) {
        System.out.println("\n=== " + geneName + " Results Comparison ===");
        System.out.println(String.format("%-15s %-45s %-45s %s", "Sample", "Expected", "Obtained", "Match"));
        System.out.println(new String(new char[130]).replace('\0', '-'));

        int totalSamples = 0;
        int exactMatches = 0;
        int bothNoTranslation = 0;

        for (Map.Entry<String, Map<String, String>> expectedEntry : expectedResults.entrySet()) {
            String sampleId = expectedEntry.getKey();
            String expected = expectedEntry.getValue().get(geneName);

            Map<String, String> sampleObtained = obtainedResults.get(sampleId);
            if (sampleObtained == null) {
                continue;
            }

            totalSamples++;
            String obtained = sampleObtained.get(geneName);

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
    public void testGenerateAnnotatedJsonLines() throws IOException {
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

        // 3. Export one JSON file per sample
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

    private String truncate(String str, int maxLength) {
        if (str == null || str.length() <= maxLength) {
            return str;
        }
        return str.substring(0, maxLength - 3) + "...";
    }
}
