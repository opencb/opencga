package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;

import java.util.Arrays;
import java.util.Collections;

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
        converter = new AnnotationIndexConverter(Arrays.asList(
                "STUDY:POP_1",
                "STUDY:POP_2",
                "STUDY:POP_3",
                "STUDY:POP_4",
                "STUDY:POP_5",
                "STUDY:POP_6"
        ));
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
    public void testPopFreq() {
        assertEquals(POP_FREQ_ANY_001_MASK | INTERGENIC_MASK,
                b = converter.convert(new VariantAnnotation()).getSummaryIndex());
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
        new AnnotationIndexConverter(Arrays.asList("1kG_phase3:ALL", "GNOMAD_GENOMES:ALL", "1kG_phase3:ALL"));
    }

    @Test
    public void testPopFreqMulti() {
        assertArrayEquals(new byte[]{0b11, 0, 0, 0, 0, 0}, converter.convert(annot(pf("STUDY", "POP_1", 0.5))).getPopFreqIndex());
        assertArrayEquals(new byte[]{0b11, 0, 0, 0, 0, 0}, converter.convert(annot(pf("STUDY", "POP_1", 0.5), pf(K_GENOMES, "ALL", 0.3))).getPopFreqIndex());
        assertArrayEquals(new byte[]{0b11, 0, 0, 0, 0, 0}, converter.convert(annot(pf("STUDY", "POP_1", 0.5), pf("STUDY", "POP_2", 0))).getPopFreqIndex());

        assertArrayEquals(new byte[]{0b11, 0b10, 0, 0, 0, 0}, converter.convert(annot(pf("STUDY", "POP_1", 0.5), pf("STUDY", "POP_2", 0.005))).getPopFreqIndex());
        assertArrayEquals(new byte[]{0b11, 0, 0, 0b11, 0b11, 0}, converter.convert(annot(pf("STUDY", "POP_1", 0.5), pf("STUDY", "POP_4", 0.5), pf("STUDY", "POP_5", 0.5))).getPopFreqIndex());
    }


    protected VariantAnnotation annot(ConsequenceType... value) {
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setConsequenceTypes(Arrays.asList(value));
        return variantAnnotation;
    }

    protected VariantAnnotation annot(PopulationFrequency... value) {
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setPopulationFrequencies(Arrays.asList(value));
        return variantAnnotation;
    }

    public PopulationFrequency pf(String study, String population, double af) {
        PopulationFrequency pf = new PopulationFrequency();
        pf.setStudy(study);
        pf.setPopulation(population);
        pf.setAltAlleleFreq((float) (af));
        pf.setRefAlleleFreq((float) (1 - af));

        return pf;
    }

    public ConsequenceType ct(String ct, String biotype) {
        ConsequenceType consequenceType = ct(ct);
        consequenceType.setGeneName("Gene");
        consequenceType.setEnsemblGeneId("ENSEMBL_GENE");
        consequenceType.setBiotype(biotype);
        return consequenceType;
    }

    public ConsequenceType ct(String ct) {
        ConsequenceType consequenceType = new ConsequenceType();;
        consequenceType.setGeneName("Gene");
        consequenceType.setEnsemblGeneId("ENSEMBL_GENE");
        consequenceType.setSequenceOntologyTerms(Collections.singletonList(new SequenceOntologyTerm(ct, ct)));
        return consequenceType;
    }

}