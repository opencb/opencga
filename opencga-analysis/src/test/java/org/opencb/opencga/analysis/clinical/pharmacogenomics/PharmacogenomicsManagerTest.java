package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class PharmacogenomicsManagerTest {

    @Test
    @Category(MediumTests.class)
    public void testAnnotateResults() throws IOException {
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .setVersion("v6.7")
                .setDefaultSpecies("hsapiens")
                .setRest(new RestConfig(Collections.singletonList("https://ws.zettagenomics.com/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient(clientConfiguration);

        ObjectMapper objectMapper = new ObjectMapper();

        InputStream is = getClass().getClassLoader().getResourceAsStream(
                "pharmacogenomics/TrueMark128_detail_result.csv.gz");
        assertNotNull("Test resource pharmacogenomics/TrueMark128_detail_result.csv.gz not found", is);
        List<AlleleTyperResult> results = buildResultsFromCsv(is);
        assertFalse("Results should not be empty", results.isEmpty());

        // Write parsed CSV results (before annotation) to /tmp, one file per sample
        Path parsedDir = Paths.get("/tmp/pgx_parsed_results");
        Files.createDirectories(parsedDir);
        for (AlleleTyperResult r : results) {
            Path samplePath = parsedDir.resolve(r.getSampleId() + ".json");
            Files.write(samplePath, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(r));
        }
        System.out.println("Parsed CSV results written to: " + parsedDir + " (" + results.size() + " files)");

        PharmacogenomicsManager manager = new PharmacogenomicsManager(null);
        manager.annotateResults(results, cellBaseClient);

        // Write annotated results to /tmp, one file per sample
        Path annotatedDir = Paths.get("/tmp/pgx_annotated_results");
        Files.createDirectories(annotatedDir);
        long totalSize = 0;
        for (AlleleTyperResult r : results) {
            byte[] json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(r);
            Path samplePath = annotatedDir.resolve(r.getSampleId() + ".json");
            Files.write(samplePath, json);
            totalSize += json.length;
        }
        System.out.println("Annotated results written to: " + annotatedDir + " (" + results.size() + " files)");
        System.out.println("Annotation total serialized size: " + String.format("%.2f", totalSize / 1024.0) + " KB");

        for (AlleleTyperResult result : results) {
            assertNotNull(result);
            if (result.getAlleleTyperResults() == null) {
                continue;
            }
            for (AlleleTyperResult.StarAlleleResult starAlleleResult : result.getAlleleTyperResults()) {
                if (starAlleleResult.getAlleleCalls() == null) {
                    continue;
                }
                for (AlleleTyperResult.AlleleCall alleleCall : starAlleleResult.getAlleleCalls()) {
                    assertNotNull("Annotation missing for gene=" + starAlleleResult.getGene()
                            + " allele=" + alleleCall.getAllele(), alleleCall.getAnnotation());
                    assertEquals("cellbase", alleleCall.getAnnotation().getSource());
                    assertNotNull(alleleCall.getAnnotation().getDrugs());
                }
            }
        }
    }

    /**
     * From the input stream builds an AlleleTyperResult list.
     * The file uses CR-only (\r) line endings.
     * Sample names: column A, rows 6–28 (1-indexed).
     * Gene names:   columns C–V (0-based indices 2–21), row 5.
     * Star alleles: columns C–V, rows 6–28.
     * NTC (negative control) rows are skipped.
     */
    public static List<AlleleTyperResult> buildResultsFromCsv(InputStream is) throws IOException {
        byte[] bytes;
        try (GZIPInputStream gzis = new GZIPInputStream(is)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = gzis.read(buf)) != -1) {
                baos.write(buf, 0, n);
            }
            bytes = baos.toByteArray();
        }

        String[] rows = new String(bytes, StandardCharsets.UTF_8).split("\r");

        // Find the header row where column A == "sample ID" (row 5 in the file)
        List<String> geneHeaders = null;
        int headerIdx = -1;
        for (int i = 0; i < rows.length; i++) {
            List<String> fields = parseCsvRow(rows[i]);
            if (!fields.isEmpty() && "sample ID".equals(fields.get(0).trim())) {
                geneHeaders = fields;
                headerIdx = i;
                break;
            }
        }
        assertNotNull("Gene header row (sample ID) not found in CSV", geneHeaders);

        List<AlleleTyperResult> results = new ArrayList<>();
        for (int i = headerIdx + 1; i < rows.length; i++) {
            List<String> fields = parseCsvRow(rows[i]);
            if (fields.isEmpty() || fields.get(0).trim().isEmpty()) {
                continue;
            }
            String sampleId = fields.get(0).trim();
            if ("NTC".equalsIgnoreCase(sampleId)) {
                continue;
            }

            List<AlleleTyperResult.StarAlleleResult> starAlleleResults = new ArrayList<>();
            for (int col = 2; col <= 21 && col < fields.size() && col < geneHeaders.size(); col++) {
                String gene = geneHeaders.get(col).trim();
                List<AlleleTyperResult.AlleleCall> alleleCalls = parseAlleleCalls(fields.get(col).trim());
                if (!alleleCalls.isEmpty()) {
                    starAlleleResults.add(new AlleleTyperResult.StarAlleleResult(gene, alleleCalls, null));
                }
            }
            results.add(new AlleleTyperResult(sampleId, starAlleleResults, null, null));
        }
        return results;
    }

    /**
     * Parses a star-allele cell value into a list of AlleleCalls.
     * Handles: "*1/*5", "{*2/*41, *21/*41}", "*4/*6, *4.003/*6", "{*1/*1,...}" (truncated).
     * Non-star-allele values (Ref/Alt, APOE E3/E4, etc.) are skipped.
     */
    public static List<AlleleTyperResult.AlleleCall> parseAlleleCalls(String rawValue) {
        List<AlleleTyperResult.AlleleCall> calls = new ArrayList<>();
        if (rawValue == null || rawValue.isEmpty()
                || "no translation available".equalsIgnoreCase(rawValue)) {
            return calls;
        }

        String value = rawValue;
        if (value.startsWith("{") && value.endsWith("}")) {
            value = value.substring(1, value.length() - 1);
        }
        // Remove truncated tail, e.g. ",...}"
        int truncIdx = value.indexOf(",...");
        if (truncIdx != -1) {
            value = value.substring(0, truncIdx);
        }

        for (String part : value.split(",\\s*")) {
            String allele = part.trim();
            if (allele.contains("*")) {
                calls.add(new AlleleTyperResult.AlleleCall(allele));
            }
        }
        return calls;
    }

    /**
     * Simple CSV row parser that correctly handles double-quoted fields containing commas.
     */
    public static List<String> parseCsvRow(String line) {
        List<String> fields = new ArrayList<>();
        if (line == null || line.isEmpty()) {
            return fields;
        }
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
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
}
