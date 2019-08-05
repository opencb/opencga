package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.query.RangeQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleFileIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.DELTA;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.EMPTY_MASK;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter.*;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser.validSampleIndexQuery;

/**
 * Created on 08/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexQueryParserTest {

    private SampleIndexQueryParser sampleIndexQueryParser;

    @Before
    public void setUp() throws Exception {
        SampleIndexConfiguration configuration = new SampleIndexConfiguration()
                .addPopulationRange(new SampleIndexConfiguration.PopulationFrequencyRange("1kG_phase3", "ALL"))
                .addPopulationRange(new SampleIndexConfiguration.PopulationFrequencyRange("GNOMAD_GENOMES", "ALL"))
                .addPopulationRange(new SampleIndexConfiguration.PopulationFrequencyRange("s1", "ALL"))
                .addPopulationRange(new SampleIndexConfiguration.PopulationFrequencyRange("s2", "ALL"))
                .addPopulationRange(new SampleIndexConfiguration.PopulationFrequencyRange("s3", "ALL"))
                .addPopulationRange(new SampleIndexConfiguration.PopulationFrequencyRange("s4", "ALL"));
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        VariantStorageMetadataManager mm = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        sampleIndexQueryParser = new SampleIndexQueryParser(mm, configuration);
        StudyMetadata study = mm.createStudy("study");
        mm.registerFile(study.getId(), "F1", Arrays.asList("S1", "S2", "S3"));
    }

    private SampleFileIndexQuery parseFileQuery(Query query, String sample, Function<String, Collection<String>> filesFromSample) {
        return sampleIndexQueryParser.parseFileQuery(query, sample, filesFromSample);
    }

    private byte parseAnnotationMask(Query query) {
        return sampleIndexQueryParser.parseAnnotationIndexQuery(query).getAnnotationIndexMask();
    }

    private byte parseAnnotationMask(Query query, boolean allSamplesAnnotated) {
        return sampleIndexQueryParser.parseAnnotationIndexQuery(query, allSamplesAnnotated).getAnnotationIndexMask();
    }

    private SampleAnnotationIndexQuery parseAnnotationIndexQuery(Query query) {
        return sampleIndexQueryParser.parseAnnotationIndexQuery(query);
    }

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
    public void parseVariantTpeQuery() {

        Query q;
        SampleIndexQuery sampleIndexQuery;

        q = new Query(TYPE.key(), "INDEL").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "INDEL,SNV,INSERTION,DELETION,CNV,BREAKEND,MNV").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "INDEL,SNV").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "SNV").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "SNV,SNP").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "SNP").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        // Filter by SNV, as it can not filter by SNP
        assertEquals(Collections.singleton(VariantType.SNV), sampleIndexQuery.getVariantTypes());
        assertEquals(VariantType.SNP.name(), q.getString(TYPE.key()));


        q = new Query(TYPE.key(), "MNV").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "MNV,MNP").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "MNP").append(SAMPLE.key(), "S1");
        sampleIndexQuery = sampleIndexQueryParser.parse(q);
        // Filter by MNV, as it can not filter by MNP
        assertEquals(Collections.singleton(VariantType.MNV), sampleIndexQuery.getVariantTypes());
        assertEquals(VariantType.MNP.name(), q.getString(TYPE.key()));

    }

    @Test
    public void parseFileFilterTest() {
        testFilter("PASS", true);
        testFilter("!PASS", false);
        testFilter("LowQual", false);
        testFilter("!LowQual", null);
        testFilter( "LowGQX,LowQual", false);
        testFilter( "LowGQX;LowQual", false);
        testFilter( "PASS,LowQual", null);
        testFilter( "PASS;LowQual", null);
        testFilter("!LowGQX;!LowQual", null);
    }

    private void testFilter(String value, Boolean pass) {
        SampleFileIndexQuery q = parseFileQuery(new Query(FILTER.key(), value), "", null);
        if (pass == null) {
            assertEquals(EMPTY_MASK, q.getFileIndexMask());
        } else {
            assertEquals(VariantFileIndexConverter.FILTER_PASS_MASK, q.getFileIndexMask());
            if (pass) {
                assertTrue(q.getValidFileIndex()[VariantFileIndexConverter.FILTER_PASS_MASK]);
            } else {
                assertFalse(q.getValidFileIndex()[VariantFileIndexConverter.FILTER_PASS_MASK]);
            }
        }
    }

    @Test
    public void parseFileQualTest() {

        for (Double qual : Arrays.asList(5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 40.0)) {
            SampleFileIndexQuery fileQuery;

            fileQuery = parseFileQuery(new Query(QUAL.key(), "=" + qual), "", null);
            assertEquals("=" + qual, new RangeQuery(qual, qual + DELTA,
                            IndexUtils.getRangeCode(qual, SampleIndexConfiguration.QUAL_THRESHOLDS),
                            IndexUtils.getRangeCodeExclusive(qual + DELTA, SampleIndexConfiguration.QUAL_THRESHOLDS)),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());

            fileQuery = parseFileQuery(new Query(QUAL.key(), "<=" + qual), "", null);
            assertEquals("<=" + qual, new RangeQuery(Double.MIN_VALUE, qual + DELTA,
                            (byte) 0,
                            IndexUtils.getRangeCodeExclusive(qual + DELTA, SampleIndexConfiguration.QUAL_THRESHOLDS)),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());

            fileQuery = parseFileQuery(new Query(QUAL.key(), "<" + qual), "", null);
            assertEquals("<" + qual, new RangeQuery(Double.MIN_VALUE, qual,
                            (byte) 0,
                            IndexUtils.getRangeCodeExclusive(qual, SampleIndexConfiguration.QUAL_THRESHOLDS)),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());

            fileQuery = parseFileQuery(new Query(QUAL.key(), ">=" + qual), "", null);
            assertEquals(">=" + qual, new RangeQuery(qual, Double.MAX_VALUE,
                            IndexUtils.getRangeCode(qual, SampleIndexConfiguration.QUAL_THRESHOLDS),
                            (byte) 4),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());

            fileQuery = parseFileQuery(new Query(QUAL.key(), ">" + qual), "", null);
            assertEquals(">" + qual, new RangeQuery(qual + DELTA, Double.MAX_VALUE,
                            IndexUtils.getRangeCode(qual + DELTA, SampleIndexConfiguration.QUAL_THRESHOLDS),
                            (byte) 4),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());
        }
    }

    @Test
    public void parseFileDPTest() {
        for (Double dp : Arrays.asList(3.0, 5.0, 10.0, 15.0, 20.0, 30.0, 35.0)) {
            for (Pair<String, String> pair : Arrays.asList(Pair.of(INFO.key(), "F1"), Pair.of(FORMAT.key(), "S1"))) {
                SampleFileIndexQuery fileQuery = parseFileQuery(new Query(pair.getKey(), pair.getValue() + ":DP=" + dp), "S1", n -> Collections.singleton("F1"));
                assertEquals("=" + dp, new RangeQuery(dp, dp + DELTA,
                                IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS),
                                IndexUtils.getRangeCodeExclusive(dp + DELTA, SampleIndexConfiguration.DP_THRESHOLDS)),
                        fileQuery.getDpQuery());
                assertEquals(VariantFileIndexConverter.DP_MASK, fileQuery.getFileIndexMask());

                fileQuery = parseFileQuery(new Query(pair.getKey(), pair.getValue() + ":DP<=" + dp), "S1", n -> Collections.singleton("F1"));
                assertEquals("<=" + dp, new RangeQuery(Double.MIN_VALUE, dp + DELTA,
                                (byte) 0,
                                IndexUtils.getRangeCodeExclusive(dp + DELTA, SampleIndexConfiguration.DP_THRESHOLDS)),
                        fileQuery.getDpQuery());
                assertEquals(VariantFileIndexConverter.DP_MASK, fileQuery.getFileIndexMask());

                fileQuery = parseFileQuery(new Query(pair.getKey(), pair.getValue() + ":DP<" + dp), "S1", n -> Collections.singleton("F1"));
                assertEquals("<" + dp, new RangeQuery(Double.MIN_VALUE, dp,
                                (byte) 0,
                                IndexUtils.getRangeCodeExclusive(dp, SampleIndexConfiguration.DP_THRESHOLDS)),
                        fileQuery.getDpQuery());
                assertEquals(VariantFileIndexConverter.DP_MASK, fileQuery.getFileIndexMask());

                fileQuery = parseFileQuery(new Query(pair.getKey(), pair.getValue() + ":DP>=" + dp), "S1", n -> Collections.singleton("F1"));
                assertEquals(">=" + dp, new RangeQuery(dp, Double.MAX_VALUE,
                                IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS),
                                (byte) 4),
                        fileQuery.getDpQuery());
                assertEquals(VariantFileIndexConverter.DP_MASK, fileQuery.getFileIndexMask());

                fileQuery = parseFileQuery(new Query(pair.getKey(), pair.getValue() + ":DP>" + dp), "S1", n -> Collections.singleton("F1"));
                assertEquals(">" + dp, new RangeQuery(dp + DELTA, Double.MAX_VALUE,
                                IndexUtils.getRangeCode(dp + DELTA, SampleIndexConfiguration.DP_THRESHOLDS),
                                (byte) 4),
                        fileQuery.getDpQuery());
                assertEquals(VariantFileIndexConverter.DP_MASK, fileQuery.getFileIndexMask());}

        }
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
        SampleAnnotationIndexQuery annotationIndexQuery = parseAnnotationIndexQuery(query);
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
        assertEquals(CT_MISSENSE_VARIANT_MASK | CT_START_LOST_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,start_lost")).getConsequenceTypeMask());
        assertEquals(CT_START_LOST_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "start_lost")).getConsequenceTypeMask());
        assertEquals(CT_STOP_GAINED_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained")).getConsequenceTypeMask());
        assertEquals(CT_STOP_LOST_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost")).getConsequenceTypeMask());
        assertEquals(CT_STOP_GAINED_MASK | CT_STOP_LOST_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained,stop_lost")).getConsequenceTypeMask());

        assertEquals(CT_MISSENSE_VARIANT_MASK | CT_STOP_LOST_MASK | CT_UTR_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost,missense_variant,3_prime_UTR_variant")).getConsequenceTypeMask());
        assertEquals(INTERGENIC_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost,missense_variant,3_prime_UTR_variant")).getAnnotationIndexMask());

        // CT Filter covered by summary
        assertEquals(EMPTY_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant")).getConsequenceTypeMask());
        assertEquals(((short) LOF_SET.stream().mapToInt(AnnotationIndexConverter::getMaskFromSoName).reduce((a, b) -> a | b).getAsInt()),
                parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), LOF_SET)).getConsequenceTypeMask());
        assertEquals(EMPTY_MASK, parseAnnotationIndexQuery(new Query(ANNOT_CONSEQUENCE_TYPE.key(), LOF_EXTENDED_SET)).getConsequenceTypeMask());

    }

    @Test
    public void parseBiotypeTypeMaskTest() {
        assertEquals(EMPTY_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "other_biotype")).getBiotypeMask());
        assertEquals(BT_PROTEIN_CODING_MASK | BT_MIRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "protein_coding,miRNA")).getBiotypeMask());
        assertEquals(BT_MIRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "miRNA")).getBiotypeMask());
        assertEquals(BT_LNCRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "lncRNA")).getBiotypeMask());
        assertEquals(BT_LNCRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "lincRNA")).getBiotypeMask());
        assertEquals(BT_LNCRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "lncRNA,lincRNA")).getBiotypeMask());

        // Ensure PROTEIN_CODING_MASK is not added to the summary
        assertEquals(INTERGENIC_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "protein_coding,miRNA")).getAnnotationIndexMask());
        assertEquals(BT_PROTEIN_CODING_MASK | BT_MIRNA_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "protein_coding,miRNA")).getBiotypeMask());

        // Biotype Filter covered by summary
        assertEquals(EMPTY_MASK, parseAnnotationIndexQuery(new Query(ANNOT_BIOTYPE.key(), "protein_coding")).getBiotypeMask());
    }

    @Test
    public void parsePopFreqQueryTest() {
        double[] default_ranges = SampleIndexConfiguration.PopulationFrequencyRange.DEFAULT_THRESHOLDS;
        for (int i = 0; i < default_ranges.length; i++) {
            SampleAnnotationIndexQuery.PopulationFrequencyQuery q;
            double r = default_ranges[i];
//            System.out.println("--------------");
//            System.out.println(r);

            final double d = 0.00001;

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL<" + (r - d))).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(0, q.getMinCodeInclusive());
            assertEquals(i + 1, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL<=" + (r - d))).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(0, q.getMinCodeInclusive());
            assertEquals(i + 1, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL<" + r)).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(0, q.getMinCodeInclusive());
            assertEquals(i + 1, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL<=" + r)).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(0, q.getMinCodeInclusive());
            assertEquals(i + 2, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL<" + (r + d))).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(0, q.getMinCodeInclusive());
            assertEquals(i + 2, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());


            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL>=" + r)).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(i + 1, q.getMinCodeInclusive());
            assertEquals(4, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL>" + r)).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(i + 1, q.getMinCodeInclusive());
            assertEquals(4, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL>" + (r + d))).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(i + 1, q.getMinCodeInclusive());
            assertEquals(4, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL>=" + (r + d))).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(i + 1, q.getMinCodeInclusive());
            assertEquals(4, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL>" + (r - d))).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(i, q.getMinCodeInclusive());
            assertEquals(4, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

            q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s" + (i % 4 + 1) + ":ALL>=" + (r - d))).getPopulationFrequencyQueries().get(0);
//            System.out.println(q);
            assertEquals(i, q.getMinCodeInclusive());
            assertEquals(4, q.getMaxCodeExclusive());
            assertEquals(i % 4 + 1 + 1, q.getPosition());

        }
        SampleAnnotationIndexQuery q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s8:NONE>0.1"));
        assertEquals(0, q.getPopulationFrequencyQueries().size());

        q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s2:ALL>0.1;s8:NONE>0.1"));
        assertEquals(1, q.getPopulationFrequencyQueries().size());
        assertEquals(QueryOperation.AND, q.getPopulationFrequencyQueryOperator());
        assertEquals(true, q.isPopulationFrequencyQueryPartial());

        // Partial OR queries can not be used
        q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s2:ALL>0.1,s8:NONE>0.1"));
        assertEquals(0, q.getPopulationFrequencyQueries().size());
        assertEquals(QueryOperation.OR, q.getPopulationFrequencyQueryOperator());
        assertEquals(true, q.isPopulationFrequencyQueryPartial());

        q = parseAnnotationIndexQuery(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s2:ALL>0.1,s3:ALL>0.1"));
        assertEquals(2, q.getPopulationFrequencyQueries().size());
        assertEquals(QueryOperation.OR, q.getPopulationFrequencyQueryOperator());
        assertEquals(false, q.isPopulationFrequencyQueryPartial());
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