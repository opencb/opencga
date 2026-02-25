package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.StarAlleleAnnotation;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class StarAlleleAnnotatorTest {

    private StarAlleleAnnotator annotator;

    @Before
    public void setUp() {
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .setVersion("v6.7")
                .setDefaultSpecies("hsapiens")
                .setRest(new RestConfig(Collections.singletonList("https://ws.zettagenomics.com/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient(clientConfiguration);
        annotator = new StarAlleleAnnotator(cellBaseClient);
    }

    @Test
    public void testAnnotateCYP2C9() throws IOException {
        StarAlleleAnnotation annotation = annotator.annotate("CYP2C9", "*4/*16");

        assertNotNull("Annotation should not be null", annotation);
        assertEquals("cellbase", annotation.getSource());
        assertEquals("v6.7", annotation.getVersion());
        assertNotNull("Drugs should not be null", annotation.getDrugs());
        assertFalse("Should have at least one drug for CYP2C9", annotation.getDrugs().isEmpty());

        System.out.println("=== CYP2C9 *4/*16 Annotation ===");
        System.out.println("Source: " + annotation.getSource());
        System.out.println("Version: " + annotation.getVersion());
        System.out.println("Number of drugs: " + annotation.getDrugs().size());
        annotation.getDrugs().forEach(drug -> {
            System.out.println("  Drug: " + drug.getName() + " (id=" + drug.getId() + ")");
            if (drug.getVariants() != null) {
                System.out.println("    Variants: " + drug.getVariants().size());
                drug.getVariants().forEach(v -> System.out.println("      - " + v.getVariantId()
                        + " haplotypes=" + v.getHaplotypes() + " geneNames=" + v.getGeneNames()));
            }
            if (drug.getGenes() != null) {
                System.out.println("    Genes: " + drug.getGenes().size());
                drug.getGenes().forEach(g -> System.out.println("      - " + g.getName()));
            }
        });
    }

    @Test
    public void testAnnotateCYP2D6() throws IOException {
        StarAlleleAnnotation annotation = annotator.annotate("CYP2D6", "*2/*41");

        assertNotNull(annotation);
        assertEquals("cellbase", annotation.getSource());
        assertNotNull(annotation.getDrugs());

        System.out.println("\n=== CYP2D6 *2/*41 Annotation ===");
        System.out.println("Number of drugs: " + annotation.getDrugs().size());
        annotation.getDrugs().forEach(drug -> {
            System.out.println("  Drug: " + drug.getName() + " (variants=" +
                    (drug.getVariants() != null ? drug.getVariants().size() : 0) + ", genes=" +
                    (drug.getGenes() != null ? drug.getGenes().size() : 0) + ")");
        });
    }

    @Test
    public void testAnnotateUnknownGene() throws IOException {
        StarAlleleAnnotation annotation = annotator.annotate("UNKNOWN_GENE_XYZ", "*1/*1");

        assertNotNull(annotation);
        assertEquals("cellbase", annotation.getSource());
        assertNotNull(annotation.getDrugs());
        assertTrue("Should have no drugs for unknown gene", annotation.getDrugs().isEmpty());
    }

    @Test
    public void testAnnotateCYP2C19() throws IOException {
        StarAlleleAnnotation annotation = annotator.annotate("CYP2C19", "*1/*7");

        assertNotNull(annotation);
        assertEquals("cellbase", annotation.getSource());
        assertNotNull(annotation.getDrugs());
        assertFalse("Should have at least one drug for CYP2C19", annotation.getDrugs().isEmpty());

        System.out.println("\n=== CYP2C19 *1/*7 Annotation ===");
        System.out.println("Number of drugs: " + annotation.getDrugs().size());
        annotation.getDrugs().forEach(drug -> {
            System.out.println("  Drug: " + drug.getName() + " (variants=" +
                    (drug.getVariants() != null ? drug.getVariants().size() : 0) + ")");
        });
    }

    @Test
    public void testAnnotateCYP2B6() throws IOException {
        StarAlleleAnnotation annotation = annotator.annotate("CYP2B6", "*5/*6");

        assertNotNull(annotation);
        assertEquals("cellbase", annotation.getSource());
        assertNotNull(annotation.getDrugs());
        assertFalse("Should have at least one drug for CYP2B6", annotation.getDrugs().isEmpty());

        System.out.println("\n=== CYP2B6 *5/*6 Annotation ===");
        System.out.println("Number of drugs: " + annotation.getDrugs().size());
        annotation.getDrugs().forEach(drug -> {
            System.out.println("  Drug: " + drug.getName() + " (variants=" +
                    (drug.getVariants() != null ? drug.getVariants().size() : 0) + ")");
        });
    }

    @Test
    public void testWriteAnnotationsToFile() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        StarAlleleAnnotation cyp2c9 = annotator.annotate("CYP2C9", "*4/*16");
        StarAlleleAnnotation cyp2c19 = annotator.annotate("CYP2C19", "*1/*7");
        StarAlleleAnnotation cyp2b6 = annotator.annotate("CYP2B6", "*5/*6");

        Files.write(Paths.get("/tmp/CYP2C9.json"), mapper.writeValueAsBytes(cyp2c9));
        Files.write(Paths.get("/tmp/CYP2C19.json"), mapper.writeValueAsBytes(cyp2c19));
        Files.write(Paths.get("/tmp/CYP2B6.json"), mapper.writeValueAsBytes(cyp2b6));
    }

    @Test
    public void testVariantsFilteredByGene() throws IOException {
        StarAlleleAnnotation annotation = annotator.annotate("CYP2C9", "*4/*16");

        // All remaining variants should belong to CYP2C9
        annotation.getDrugs().forEach(drug -> {
            if (drug.getVariants() != null) {
                drug.getVariants().forEach(v -> {
                    assertTrue("Variant geneNames should contain CYP2C9",
                            v.getGeneNames() != null && v.getGeneNames().contains("CYP2C9"));
                });
            }
        });
    }
}
