package org.opencb.opencga.analysis.clinical.pharmacogenomics;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class PharmacogenomicsManagerTest {

    private String genotypingContent;
    private String translationContent;

//    @Before
//    public void setUp() throws IOException {
//        // Load file contents as strings
//        Path genotypingFile = Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/3G_Export_DO_TrueMark_128_Export_DO_TrueMark_128_Genotyping_07-11-2025-074600.txt");
//        Path translationFile = Paths.get("/home/imedina/projects/SESPA/pgx/Farmacogenetica/Archivos_experimentos/TrueMark (con CNV)/Ejemplo Thermo Fisher/PGX_SNP_CNV_128_OA_translation_RevC.csv");
//
//        genotypingContent = readFileContent(genotypingFile);
//        translationContent = readFileContent(translationFile);
//    }

//    private String readFileContent(Path filePath) throws IOException {
//        StringBuilder content = new StringBuilder();
//        try (BufferedReader br = new BufferedReader(new FileReader(filePath.toFile()))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                content.append(line).append("\n");
//            }
//        }
//        return content.toString();
//    }

//    @Test
//    public void testAlleleTyperWithStringInputs() throws IOException {
//        // Test AlleleTyper with String inputs (without CatalogManager)
//        AlleleTyper typer = new AlleleTyper();
//
//        // Parse translation from string
//        typer.parseTranslationFromString(translationContent);
//
//        // Build results from string
//        List<AlleleTyperResult> results = typer.buildAlleleTyperResultsFromString(genotypingContent);
//
//        // Verify results
//        assertNotNull("Results should not be null", results);
//        assertTrue("Should have multiple samples", results.size() > 0);
//
//        System.out.println("\n=== AlleleTyper String-based Processing ===");
//        System.out.println("Total samples processed: " + results.size());
//
//        // Check first sample
//        if (!results.isEmpty()) {
//            AlleleTyperResult firstResult = results.get(0);
//            System.out.println("First sample: " + firstResult.getSampleId());
//            System.out.println("  Genes with calls: " + firstResult.getStarAlleles().size());
//            System.out.println("  Total genotypes: " + firstResult.getGenotypes().size());
//            System.out.println("  Translation genes: " + firstResult.getTranslation().size());
//
//            assertNotNull("Sample ID should not be null", firstResult.getSampleId());
//            assertNotNull("Star alleles should not be null", firstResult.getStarAlleles());
//            assertNotNull("Genotypes should not be null", firstResult.getGenotypes());
//            assertNotNull("Translation should not be null", firstResult.getTranslation());
//        }
//    }

//    @Test
//    public void testPharmacogenomicsManagerCreation() {
//        // Test that PharmacogenomicsManager can be created without CatalogManager
//        // (for unit testing purposes)
//        // Note: In production, CatalogManager would be provided
//
//        System.out.println("\n=== PharmacogenomicsManager Test ===");
//        System.out.println("PharmacogenomicsManager requires CatalogManager for full functionality");
//        System.out.println("Allele typing can be performed independently with AlleleTyper");
//        System.out.println("Results can be stored in catalog via PharmacogenomicsManager.alleleTyper()");
//
//        assertTrue("PharmacogenomicsManager class should exist", true);
//    }

//    @Test
//    public void testStringBasedWorkflow() throws IOException {
//        System.out.println("\n=== String-based Workflow Demonstration ===");
//
//        // Step 1: Parse translation
//        AlleleTyper typer = new AlleleTyper();
//        typer.parseTranslationFromString(translationContent);
//        System.out.println("✓ Translation file parsed from String");
//
//        // Step 2: Process genotyping data
//        List<AlleleTyperResult> results = typer.buildAlleleTyperResultsFromString(genotypingContent);
//        System.out.println("✓ Genotyping data processed from String");
//        System.out.println("✓ Generated " + results.size() + " sample results");
//
//        // Step 3: Export to JSON (demonstrating the workflow)
//        if (!results.isEmpty()) {
//            String json = typer.exportToJson(results.get(0));
//            System.out.println("✓ Results can be exported to JSON");
//            System.out.println("  JSON size: " + json.length() + " characters");
//
//            assertTrue("JSON should contain sampleId", json.contains("sampleId"));
//            assertTrue("JSON should contain starAlleles", json.contains("starAlleles"));
//            assertTrue("JSON should contain genotypes", json.contains("genotypes"));
//        }
//
//        System.out.println("\n=== Workflow Complete ===");
//        System.out.println("In production:");
//        System.out.println("1. PharmacogenomicsManager receives String content");
//        System.out.println("2. AlleleTyper processes the data");
//        System.out.println("3. Results stored as sample attributes in Catalog");
//    }
}
