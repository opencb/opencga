package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter.*;

/**
 * Created on 17/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AnnotationIndexConverterTest {

    private AnnotationIndexConverter converter;
    byte b;

    @Before
    public void setUp() throws Exception {
        List<String> populations = Arrays.asList(
                "STUDY:POP_1",
                "STUDY:POP_2",
                "STUDY:POP_3",
                "STUDY:POP_4",
                "STUDY:POP_5",
                "STUDY:POP_6"
        );
        SampleIndexConfiguration configuration = new SampleIndexConfiguration().setPopulationRanges(
                populations.stream().map(SampleIndexConfiguration.PopulationFrequencyRange::new).collect(Collectors.toList()));

        converter = new AnnotationIndexConverter(configuration);
    }

//    @After
//    public void tearDown() throws Exception {
//        System.out.println(IndexUtils.byteToString(b));
//    }

    @Test
    public void testLof() {
        assertEquals(LOF_MASK | LOF_EXTENDED_MASK | POP_FREQ_ANY_001_MASK,
                b = converter.convert(annot(ct("stop_lost"))).getSummaryIndex());
    }

    @Test
    public void testLofe() {
        assertEquals(LOF_EXTENDED_MASK | MISSENSE_VARIANT_MASK | POP_FREQ_ANY_001_MASK,
                b = converter.convert(annot(ct("missense_variant"))).getSummaryIndex());
    }

    @Test
    public void testLofProtein() {
        assertEquals(LOF_MASK | LOF_EXTENDED_MASK | LOFE_PROTEIN_CODING_MASK | PROTEIN_CODING_MASK | POP_FREQ_ANY_001_MASK,
                b = converter.convert(annot(ct("stop_lost", "protein_coding"))).getSummaryIndex());
    }

    @Test
    public void testLofeProtein() {
        assertEquals(LOF_EXTENDED_MASK | MISSENSE_VARIANT_MASK | LOFE_PROTEIN_CODING_MASK | PROTEIN_CODING_MASK | POP_FREQ_ANY_001_MASK,
                b = converter.convert(annot(ct("missense_variant", "protein_coding"))).getSummaryIndex());
    }

    @Test
    public void testLofProteinDifferentTranscript() {
        assertEquals(LOF_MASK | LOF_EXTENDED_MASK | PROTEIN_CODING_MASK | POP_FREQ_ANY_001_MASK,
                b = converter.convert(annot(ct("stop_lost", "other"), ct("other", "protein_coding"))).getSummaryIndex());

    }

    @Test
    public void testLofeProteinDifferentTranscript() {
        assertEquals(LOF_EXTENDED_MASK | MISSENSE_VARIANT_MASK | PROTEIN_CODING_MASK | POP_FREQ_ANY_001_MASK,
                b = converter.convert(annot(ct("missense_variant", "other"), ct("other", "protein_coding"))).getSummaryIndex());
    }

    @Test
    public void testCtBtCombination() {
        AnnotationIndexEntry entry = converter.convert(annot(ct("missense_variant", "pseudogene"), ct("pseudogene", "protein_coding")));
        byte[] ctBtIndex = entry.getCtBtMatrix();
        assertEquals(1, ctBtIndex.length);
        assertEquals(1, entry.getNumCts());
        assertEquals(1, entry.getNumBts());
        assertEquals(0, ctBtIndex[0]); // No combination

        entry = converter.convert(annot(ct("missense_variant", "protein_coding"), ct("stop_lost", "protein_coding")));
        ctBtIndex = entry.getCtBtMatrix();
        assertEquals(2, ctBtIndex.length);
        assertEquals(2, entry.getNumCts());
        assertEquals(1, entry.getNumBts());
        assertEquals(1, ctBtIndex[0]); // missense_variant
        assertEquals(1, ctBtIndex[1]); // stop_lost

        entry = converter.convert(annot(ct("missense_variant", "protein_coding"), ct("stop_lost", "protein_coding"), ct("stop_gained", "pseudogene")));
        ctBtIndex = entry.getCtBtMatrix();
        assertEquals(3, ctBtIndex.length);
        assertEquals(3, entry.getNumCts());
        assertEquals(1, entry.getNumBts());
        assertEquals(1, ctBtIndex[0]); // missense_variant
        assertEquals(0, ctBtIndex[1]); // stop_gained
        assertEquals(1, ctBtIndex[2]); // stop_lost

        entry = converter.convert(annot(
                ct("missense_variant", "protein_coding"),
                ct("start_lost", "processed_transcript"),
                ct("start_lost", "protein_coding"),
                ct("stop_lost", "processed_transcript"),
                ct("stop_gained", "pseudogene")));
        ctBtIndex = entry.getCtBtMatrix();
        assertEquals(4, ctBtIndex.length);
        assertEquals(4, entry.getNumCts());
        assertEquals(2, entry.getNumBts()); // protein_coding + processed_transcript. biotype "other" does not count

                 //protein_coding | processed_transcript
        assertEquals(0b10, ctBtIndex[0]); // missense_variant
        assertEquals(0b11, ctBtIndex[1]); // start_lost
        assertEquals(0b00, ctBtIndex[2]); // stop_gained
        assertEquals(0b01, ctBtIndex[3]); // stop_lost
    }

    @Test
    public void testIntergenic() {
        assertEquals(POP_FREQ_ANY_001_MASK | INTERGENIC_MASK,
                b = converter.convert(annot(ct("intergenic_variant"))).getSummaryIndex());
    }

    @Test
    public void testNonIntergenic() {
        // Intergenic and regulatory variants should be marked as intergenic
        assertEquals(POP_FREQ_ANY_001_MASK | INTERGENIC_MASK,
                b = converter.convert(annot(ct("intergenic_variant"), ct("regulatory_region_variant"))).getSummaryIndex());

        assertEquals(POP_FREQ_ANY_001_MASK | MISSENSE_VARIANT_MASK | LOF_EXTENDED_MASK | INTERGENIC_MASK,
                b = converter.convert(annot(ct("intergenic_variant"), ct("missense_variant"))).getSummaryIndex());
    }

    @Test
    public void testPopFreq() {
        assertEquals(POP_FREQ_ANY_001_MASK | INTERGENIC_MASK,
                b = converter.convert(annot()).getSummaryIndex());
    }

    @Test
    public void testPopFreqAny() {
        assertEquals(POP_FREQ_ANY_001_MASK | INTERGENIC_MASK,
                b = converter.convert(annot(pf(GNOMAD_GENOMES, "ALL", 0.3))).getSummaryIndex());
    }

    @Test
    public void testPopFreqNone() {
        assertEquals(INTERGENIC_MASK,
                b = converter.convert(annot(pf(GNOMAD_GENOMES, "ALL", 0.3), pf(K_GENOMES, "ALL", 0.3))).getSummaryIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicatedPopulations() {
        List<String> populations = Arrays.asList("1kG_phase3:ALL", "GNOMAD_GENOMES:ALL", "1kG_phase3:ALL");
        SampleIndexConfiguration configuration = new SampleIndexConfiguration().setPopulationRanges(
                populations.stream().map(SampleIndexConfiguration.PopulationFrequencyRange::new).collect(Collectors.toList()));
        new AnnotationIndexConverter(configuration);
    }

    @Test
    public void testPopFreqMulti() {
        assertArrayEquals(new byte[]{0b11, 0, 0, 0, 0, 0},
                converter.convert(annot(pf("STUDY", "POP_1", 0.5))).getPopFreqIndex());
        assertArrayEquals(new byte[]{0b11, 0, 0, 0, 0, 0},
                converter.convert(annot(pf("STUDY", "POP_1", 0.5), pf(K_GENOMES, "ALL", 0.3))).getPopFreqIndex());
        assertArrayEquals(new byte[]{0b11, 0, 0, 0, 0, 0},
                converter.convert(annot(pf("STUDY", "POP_1", 0.5), pf("STUDY", "POP_2", 0))).getPopFreqIndex());

        assertArrayEquals(new byte[]{0b11, 0b10, 0, 0, 0, 0},
                converter.convert(annot(pf("STUDY", "POP_1", 0.5), pf("STUDY", "POP_2", 0.00501))).getPopFreqIndex());
        assertArrayEquals(new byte[]{0b11, 0, 0, 0b01, 0b11, 0},
                converter.convert(annot(pf("STUDY", "POP_1", 0.5), pf("STUDY", "POP_4", 0.001), pf("STUDY", "POP_5", 0.5))).getPopFreqIndex());
    }

    public static VariantAnnotation annot() {
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setConsequenceTypes(Arrays.asList(ct("intergenic_variant")));
        return variantAnnotation;
    }

    public static VariantAnnotation annot(ConsequenceType... value) {
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setConsequenceTypes(Arrays.asList(value));
        return variantAnnotation;
    }

    public static VariantAnnotation annot(PopulationFrequency... value) {
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setPopulationFrequencies(Arrays.asList(value));
        variantAnnotation.setConsequenceTypes(Arrays.asList(ct("intergenic_variant")));
        return variantAnnotation;
    }

    public static PopulationFrequency pf(String study, String population, double af) {
        PopulationFrequency pf = new PopulationFrequency();
        pf.setStudy(study);
        pf.setPopulation(population);
        pf.setAltAlleleFreq((float) (af));
        pf.setRefAlleleFreq((float) (1 - af));

        return pf;
    }

    public static ConsequenceType ct(String ct, String biotype) {
        ConsequenceType consequenceType = ct(ct);
        consequenceType.setBiotype(biotype);
        return consequenceType;
    }

    public static ConsequenceType ct(String ct) {
        ConsequenceType consequenceType = new ConsequenceType();;
        consequenceType.setGeneName("Gene");
        consequenceType.setEnsemblGeneId("ENSEMBL_GENE");
        consequenceType.setSequenceOntologyTerms(Collections.singletonList(new SequenceOntologyTerm(ct, ct)));
        return consequenceType;
    }

}