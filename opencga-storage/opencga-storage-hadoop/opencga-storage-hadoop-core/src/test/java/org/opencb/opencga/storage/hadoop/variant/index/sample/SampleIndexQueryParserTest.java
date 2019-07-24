package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;

import java.util.ArrayList;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.AND;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.OR;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.EMPTY_MASK;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter.*;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser.*;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser.parseAnnotationMask;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser.parseFileMask;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexToHBaseConverter.*;

/**
 * Created on 08/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexQueryParserTest {

    @Test
    public void validSampleIndexQueryTest() {
        // Single sample
        assertTrue(validSampleIndexQuery(new Query(SAMPLE.key(), "S1")));
        assertTrue(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:1/1")));
        assertTrue(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:./1")));
        assertTrue(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:1/1,0/1")));
        assertFalse(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:!1/1"))); // Negated
        assertFalse(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:0/0")));
        assertFalse(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:./0")));
        assertFalse(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:./.")));
        assertFalse(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:1/1,./.")));

        // ALL samples (and)
        assertTrue(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:1/1,./.;S2:0/1"))); // Any valid
        assertFalse(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:1/1,./.;S2:./.")));

        // ANY sample (or)
        assertTrue(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:1/1,S2:0/1"))); // all must be valid
        assertFalse(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:1/1,./.,S2:0/1")));
        assertFalse(validSampleIndexQuery(new Query(GENOTYPE.key(), "S1:1/1,./.,S2:./.")));

    }

    @Test
    public void parseFileMaskTest() {
        assertArrayEquals(new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(), "", null));
        assertArrayEquals(new byte[]{SNV_MASK, SNV_MASK}, parseFileMask(new Query(TYPE.key(), "SNV"), "", null));
        assertArrayEquals(new byte[]{SNV_MASK, SNV_MASK}, parseFileMask(new Query(TYPE.key(), "SNP"), "", null));
        assertArrayEquals(new byte[]{SNV_MASK, EMPTY_MASK}, parseFileMask(new Query(TYPE.key(), "INDEL"), "", null));
        assertArrayEquals(new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(TYPE.key(), "SNV,INDEL"), "", null));

        assertArrayEquals(new byte[]{FILTER_PASS_MASK, FILTER_PASS_MASK}, parseFileMask(new Query(FILTER.key(), "PASS"), "", null));
        assertArrayEquals(new byte[]{FILTER_PASS_MASK, EMPTY_MASK}, parseFileMask(new Query(FILTER.key(), "!PASS"), "", null));
        assertArrayEquals(new byte[]{FILTER_PASS_MASK, EMPTY_MASK}, parseFileMask(new Query(FILTER.key(), "LowQual"), "", null));
        assertArrayEquals(new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FILTER.key(), "!LowQual"), "", null));

        assertArrayEquals(new byte[]{FILTER_PASS_MASK, EMPTY_MASK}, parseFileMask(new Query(FILTER.key(), "LowGQX,LowQual"), "", null));
        assertArrayEquals(new byte[]{FILTER_PASS_MASK, EMPTY_MASK}, parseFileMask(new Query(FILTER.key(), "LowGQX;LowQual"), "", null));
        assertArrayEquals(new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FILTER.key(), "PASS,LowQual"), "", null));
        assertArrayEquals(new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FILTER.key(), "PASS;LowQual"), "", null));
        assertArrayEquals(new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FILTER.key(), "!LowGQX;!LowQual"), "", null));


        byte qual20_40 = QUAL_GT_20_MASK | QUAL_GT_40_MASK;
        assertArrayEquals("=10", new byte[]{qual20_40, EMPTY_MASK}, parseFileMask(new Query(QUAL.key(), "=10"), "", null));
        assertArrayEquals("=20", new byte[]{qual20_40, EMPTY_MASK}, parseFileMask(new Query(QUAL.key(), "=20"), "", null));
        assertArrayEquals("=30", new byte[]{qual20_40, QUAL_GT_20_MASK}, parseFileMask(new Query(QUAL.key(), "=30"), "", null));
        assertArrayEquals("=40", new byte[]{qual20_40, QUAL_GT_20_MASK}, parseFileMask(new Query(QUAL.key(), "=40"), "", null));
        assertArrayEquals("=50", new byte[]{qual20_40, qual20_40}, parseFileMask(new Query(QUAL.key(), "=50"), "", null));

        assertArrayEquals("<=10", new byte[]{qual20_40, EMPTY_MASK}, parseFileMask(new Query(QUAL.key(), "<=10"), "", null));
        assertArrayEquals("<=20", new byte[]{qual20_40, EMPTY_MASK}, parseFileMask(new Query(QUAL.key(), "<=20"), "", null));
        assertArrayEquals("<=30", new byte[]{QUAL_GT_40_MASK, EMPTY_MASK}, parseFileMask(new Query(QUAL.key(), "<=30"), "", null));
        assertArrayEquals("<=40", new byte[]{QUAL_GT_40_MASK, EMPTY_MASK}, parseFileMask(new Query(QUAL.key(), "<=40"), "", null));

        assertArrayEquals(">=10", new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(QUAL.key(), ">=10"), "", null));
        assertArrayEquals(">=20", new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(QUAL.key(), ">=20"), "", null));
        assertArrayEquals(">=30", new byte[]{QUAL_GT_20_MASK, QUAL_GT_20_MASK}, parseFileMask(new Query(QUAL.key(), ">=30"), "", null));
        assertArrayEquals(">=40", new byte[]{QUAL_GT_20_MASK, QUAL_GT_20_MASK}, parseFileMask(new Query(QUAL.key(), ">=40"), "", null));
        assertArrayEquals(">=50", new byte[]{qual20_40, qual20_40}, parseFileMask(new Query(QUAL.key(), ">=50"), "", null));



        assertArrayEquals("S1:DP>10", new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:DP>10"), "S1", null));
        assertArrayEquals("S1:DP>=20", new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:DP>=20"), "S1", null));
        assertArrayEquals("S1:DP>=20", new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:DP>=20"), "S1", null));
        assertArrayEquals("S1:DP>20", new byte[]{DP_GT_20_MASK, DP_GT_20_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:DP>20"), "S1", null));
        assertArrayEquals("S1:DP>20", new byte[]{DP_GT_20_MASK, DP_GT_20_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:GG>4,DP>20"), "S1", null));
        assertArrayEquals("S1:DP>20", new byte[]{DP_GT_20_MASK, DP_GT_20_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:GG>4;DP>20"), "S1", null));

        assertArrayEquals("S1:DP<10", new byte[]{DP_GT_20_MASK, EMPTY_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:DP<10"), "S1", null));

        assertArrayEquals("S1:DP>20", new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:DP>20"), "S2", null));
        assertArrayEquals("S1:DP<10", new byte[]{EMPTY_MASK, EMPTY_MASK}, parseFileMask(new Query(FORMAT.key(), "S1:DP<10"), "S2", null));

    }

    @Test
    public void parseAnnotationMaskTest() {
        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query()));
        for (VariantType value : VariantType.values()) {
            assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(TYPE.key(), value)));
        }

        assertEquals(INTERGENIC_MASK | PROTEIN_CODING_MASK, parseAnnotationMask(new Query(ANNOT_BIOTYPE.key(), "protein_coding")));
        assertEquals(INTERGENIC_MASK, parseAnnotationMask(new Query(ANNOT_BIOTYPE.key(), "protein_coding,miRNA"))); // Do not use PROTEIN_CODING_MASK when filtering by other biotypes
        assertEquals(INTERGENIC_MASK, parseAnnotationMask(new Query(ANNOT_BIOTYPE.key(), "other_than_protein_coding")));
        assertEquals(LOF_EXTENDED_MASK, parseAnnotationMask(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.1")));
        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<<0.1")));

        assertEquals(INTERGENIC_MASK | MISSENSE_VARIANT_MASK | LOF_EXTENDED_MASK, parseAnnotationMask(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant")));
        assertEquals(INTERGENIC_MASK | LOF_EXTENDED_MASK, parseAnnotationMask(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost,missense_variant"))); // Do not use MISSENSE_VARIANT_MASK when filtering by other CTs
        assertEquals(INTERGENIC_MASK, parseAnnotationMask(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost,missense_variant,3_prime_UTR_variant"))); // Do not use LOF_MASK  nor LOF_EXTENDED_MASK when filtering by other CTs
        assertEquals(INTERGENIC_MASK | LOF_MASK | LOF_EXTENDED_MASK, parseAnnotationMask(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost")));
        assertEquals(INTERGENIC_MASK | LOF_MASK | LOF_EXTENDED_MASK, parseAnnotationMask(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost,stop_gained")));

//        assertEquals(INTERGENIC_MASK | LOF_EXTENDED_MASK | LOF_EXTENDED_BASIC_MASK, parseAnnotationMask(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_lost,stop_gained")
//                .append(ANNOT_TRANSCRIPT_FLAG.key(), "basic")));
//        assertEquals(INTERGENIC_MASK | LOF_MASK | LOF_EXTENDED_MASK | LOF_EXTENDED_BASIC_MASK, parseAnnotationMask(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost,stop_gained")
//                .append(ANNOT_TRANSCRIPT_FLAG.key(), "basic")));

        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.01")));
        assertEquals(POP_FREQ_ANY_001_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.001")));
        assertEquals(POP_FREQ_ANY_001_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "GNOMAD_GENOMES:ALL<0.001")));
        assertEquals(POP_FREQ_ANY_001_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "GNOMAD_GENOMES:ALL<0.0005")));
        assertEquals(POP_FREQ_ANY_001_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.001,GNOMAD_GENOMES:ALL<0.001")));
        assertEquals(POP_FREQ_ANY_001_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.001;GNOMAD_GENOMES:ALL<0.001")));

        // Mix
        assertEquals(POP_FREQ_ANY_001_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.1;GNOMAD_GENOMES:ALL<0.001")));
        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.1,GNOMAD_GENOMES:ALL<0.001")));
        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.1;GNOMAD_GENOMES:ALL<0.1")));


        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                  "GNOMAD_EXOMES:AFR<0.01;"
                + "GNOMAD_EXOMES:AMR<0.01;"
                + "GNOMAD_EXOMES:EAS<0.01;"
                + "GNOMAD_EXOMES:FIN<0.01;"
                + "GNOMAD_EXOMES:NFE<0.01;"
                + "GNOMAD_EXOMES:ASJ<0.01;"
                + "GNOMAD_EXOMES:OTH<0.01;"
                + "1kG_phase3:AFR<0.01;"
                + "1kG_phase3:AMR<0.01;"
                + "1kG_phase3:EAS<0.01;"
                + "1kG_phase3:EUR<0.01;"
                + "1kG_phase3:SAS<0.01")));

        // Removing any populations should not use the filter
        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                  "GNOMAD_EXOMES:AFR<0.01;"
                + "GNOMAD_EXOMES:AMR<0.01;"
//                + "GNOMAD_EXOMES:EAS<0.01;"
                + "GNOMAD_EXOMES:FIN<0.01;"
                + "GNOMAD_EXOMES:NFE<0.01;"
                + "GNOMAD_EXOMES:ASJ<0.01;"
                + "GNOMAD_EXOMES:OTH<0.01;"
                + "1kG_phase3:AFR<0.01;"
                + "1kG_phase3:AMR<0.01;"
                + "1kG_phase3:EAS<0.01;"
                + "1kG_phase3:EUR<0.01;"
                + "1kG_phase3:SAS<0.01")));

        // With OR instead of AND should not use the filter
        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                  "GNOMAD_EXOMES:AFR<0.01,"
                + "GNOMAD_EXOMES:AMR<0.01,"
                + "GNOMAD_EXOMES:EAS<0.01,"
                + "GNOMAD_EXOMES:FIN<0.01,"
                + "GNOMAD_EXOMES:NFE<0.01,"
                + "GNOMAD_EXOMES:ASJ<0.01,"
                + "GNOMAD_EXOMES:OTH<0.01,"
                + "1kG_phase3:AFR<0.01,"
                + "1kG_phase3:AMR<0.01,"
                + "1kG_phase3:EAS<0.01,"
                + "1kG_phase3:EUR<0.01,"
                + "1kG_phase3:SAS<0.01")));

        // With any filter greater than 0.1, should not use the filter
        assertEquals(EMPTY_MASK, parseAnnotationMask(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                  "GNOMAD_EXOMES:AFR<0.01;"
                + "GNOMAD_EXOMES:AMR<0.02;" // Increased from 0.01 to 0.02
                + "GNOMAD_EXOMES:EAS<0.01;"
                + "GNOMAD_EXOMES:FIN<0.01;"
                + "GNOMAD_EXOMES:NFE<0.01;"
                + "GNOMAD_EXOMES:ASJ<0.01;"
                + "GNOMAD_EXOMES:OTH<0.01;"
                + "1kG_phase3:AFR<0.01;"
                + "1kG_phase3:AMR<0.01;"
                + "1kG_phase3:EAS<0.01;"
                + "1kG_phase3:EUR<0.01;"
                + "1kG_phase3:SAS<0.01")));

    }

    @Test
    public void parseIntergenicTest() {
        checkIntergenic(null, new Query());

        checkIntergenic(false, new Query(ANNOT_BIOTYPE.key(), "protein_coding"));
        checkIntergenic(false, new Query(ANNOT_BIOTYPE.key(), "miRNA"));

        checkIntergenic(false, new Query(GENE.key(), "BRCA2"));
        checkIntergenic(false, new Query(ANNOT_XREF.key(), "BRCA2"));
        checkIntergenic(false, new Query(ANNOT_XREF.key(), "ENSG000000"));
        checkIntergenic(false, new Query(ANNOT_XREF.key(), "ENST000000"));

        checkIntergenic(null, new Query(ANNOT_XREF.key(), "1:1234:A:C"));
        checkIntergenic(null, new Query(ANNOT_XREF.key(), "rs12345"));
        checkIntergenic(null, new Query(ID.key(), "1:1234:A:C"));
        checkIntergenic(null, new Query(ID.key(), "rs12345"));

        checkIntergenic(null, new Query(GENE.key(), "BRCA2").append(REGION.key(), "1:123-1234"));
        checkIntergenic(null, new Query(GENE.key(), "BRCA2").append(ID.key(), "rs12345"));
        checkIntergenic(false, new Query(GENE.key(), "BRCA2").append(REGION.key(), "1:123-1234").append(ANNOT_BIOTYPE.key(), "protein_coding"));
        checkIntergenic(false, new Query(GENE.key(), "BRCA2").append(ID.key(), "rs12345").append(ANNOT_BIOTYPE.key(), "protein_coding"));

        checkIntergenic(false, new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));
        checkIntergenic(false, new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,synonymous_variant"));
        checkIntergenic(true, new Query(ANNOT_CONSEQUENCE_TYPE.key(), "intergenic_variant"));
        checkIntergenic(null, new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,intergenic_variant"));
        checkIntergenic(null, new Query(ANNOT_CONSEQUENCE_TYPE.key(), "intergenic_variant,missense_variant"));

        // Nonsense combination
        checkIntergenic(false, new Query(ANNOT_CONSEQUENCE_TYPE.key(), "intergenic_variant").append(ANNOT_BIOTYPE.key(), "protein_coding"));
    }

    private void checkIntergenic(Boolean intergenic, Query query) {
        SampleIndexQuery.SampleAnnotationIndexQuery annotationIndexQuery = parseAnnotationIndexQuery(query);
        if (intergenic == null) {
            assertEquals(EMPTY_MASK, INTERGENIC_MASK & annotationIndexQuery.getAnnotationIndexMask());
        } else {
            assertEquals(INTERGENIC_MASK, INTERGENIC_MASK & annotationIndexQuery.getAnnotationIndexMask());
            if (intergenic) {
                assertEquals(INTERGENIC_MASK, INTERGENIC_MASK & annotationIndexQuery.getAnnotationIndex());
            } else {
                assertEquals(EMPTY_MASK, INTERGENIC_MASK & annotationIndexQuery.getAnnotationIndex());
            }
        }
    }

    @Test
    public void parseConsequenceTypeMaskTest() {
        assertEquals(EMPTY_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "synonymous_variant")).getConsequenceTypeMask());
        assertEquals(CT_MISSENSE_VARIANT_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant")).getConsequenceTypeMask());
        assertEquals(CT_START_LOST_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "start_lost")).getConsequenceTypeMask());
        assertEquals(CT_STOP_GAINED_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained")).getConsequenceTypeMask());
        assertEquals(CT_STOP_LOST_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost")).getConsequenceTypeMask());
        assertEquals(CT_STOP_GAINED_MASK | CT_STOP_LOST_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained,stop_lost")).getConsequenceTypeMask());

        assertEquals(CT_MISSENSE_VARIANT_MASK | CT_STOP_LOST_MASK | CT_UTR_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost,missense_variant,3_prime_UTR_variant")).getConsequenceTypeMask());
        assertEquals(INTERGENIC_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost,missense_variant,3_prime_UTR_variant")).getAnnotationIndexMask());
    }

    @Test
    public void parseBiotypeTypeMaskTest() {
        assertEquals(EMPTY_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "other_biotype")).getBiotypeMask());
        assertEquals(BT_PROTEIN_CODING_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "protein_coding")).getBiotypeMask());
        assertEquals(BT_MIRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "miRNA")).getBiotypeMask());
        assertEquals(BT_LNCRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "lncRNA")).getBiotypeMask());
        assertEquals(BT_LNCRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "lincRNA")).getBiotypeMask());
        assertEquals(BT_LNCRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "lncRNA,lincRNA")).getBiotypeMask());

        // Ensure PROTEIN_CODING_MASK is not added to the summary
        assertEquals(INTERGENIC_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "protein_coding,miRNA")).getAnnotationIndexMask());
        assertEquals(BT_PROTEIN_CODING_MASK | BT_MIRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "protein_coding,miRNA")).getBiotypeMask());
    }

    @Test
    public void testCoveredQuery() {
        Query query;

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");
        parseAnnotationMask(query, true);
        assertFalse(query.isEmpty());

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_SET));
        parseAnnotationMask(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET));
        parseAnnotationMask(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET));
        parseAnnotationMask(query, false);
        assertFalse(query.isEmpty()); // Not all samples annotated

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(2, 4)));
        parseAnnotationMask(query, true);
        assertFalse(query.isEmpty());

        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), String.join(OR, new ArrayList<>(AnnotationIndexConverter.POP_FREQ_ANY_001_FILTERS)));
        parseAnnotationMask(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), String.join(AND, new ArrayList<>(AnnotationIndexConverter.POP_FREQ_ANY_001_FILTERS)));
        parseAnnotationMask(query, true);
        assertFalse(query.isEmpty());

        query = new Query().append(ANNOT_BIOTYPE.key(), "protein_coding");
        parseAnnotationMask(query, true);
        assertTrue(query.isEmpty());


        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET))
                .append(GENE.key(), "BRCA2");
        parseAnnotationMask(query, true); // Filtering by ct + gene
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_CONSEQUENCE_TYPE));
        assertTrue(VariantQueryUtils.isValidParam(query, GENE));

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET))
                .append(ANNOT_BIOTYPE.key(), "protein_coding");
        parseAnnotationMask(query, true); // Filtering by ct + biotype
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_CONSEQUENCE_TYPE));
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_BIOTYPE));

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET))
                .append(ANNOT_BIOTYPE.key(), BIOTYPE_SET);
        parseAnnotationMask(query, true); // Filtering by ct + biotype
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_CONSEQUENCE_TYPE));
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_BIOTYPE));
    }
}