package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.query.RangeQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleFileIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;

import java.util.*;
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
    private VariantStorageMetadataManager mm;
    private int studyId;

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
        mm = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        sampleIndexQueryParser = new SampleIndexQueryParser(mm, configuration);
        studyId = mm.createStudy("study").getId();
        mm.addIndexedFiles(studyId, Arrays.asList(mm.registerFile(studyId, "F1", Arrays.asList("S1", "S2", "S3"))));

        mm.addIndexedFiles(studyId, Arrays.asList(mm.registerFile(studyId, "fam1", Arrays.asList("fam1_child", "fam1_father", "fam1_mother"))));
        mm.addIndexedFiles(studyId, Arrays.asList(mm.registerFile(studyId, "fam2_child", Arrays.asList("fam2_child"))));
        mm.addIndexedFiles(studyId, Arrays.asList(mm.registerFile(studyId, "fam2_father", Arrays.asList("fam2_father"))));
        mm.addIndexedFiles(studyId, Arrays.asList(mm.registerFile(studyId, "fam2_mother", Arrays.asList("fam2_mother"))));
        for (String family : Arrays.asList("fam1", "fam2")) {
            mm.updateSampleMetadata(studyId, mm.getSampleId(studyId, family + "_child"),
                    sampleMetadata -> sampleMetadata
                            .setFamilyIndexStatus(TaskMetadata.Status.READY)
                            .setFather(mm.getSampleId(studyId, family + "_father"))
                            .setMother(mm.getSampleId(studyId, family + "_mother")));
        }

        mm.updateStudyMetadata("study", studyMetadata -> {
            studyMetadata.getVariantHeader().getComplexLines().add(new VariantFileHeaderComplexLine("FORMAT", "DP", "", "1", "", Collections.emptyMap()));
            return studyMetadata;
        });
    }

    private SampleIndexQuery parse(final Query query) {
        Query newQuery = new VariantQueryParser(null, mm).preProcessQuery(query, new QueryOptions());
        query.clear();
        query.putAll(newQuery);
        return sampleIndexQueryParser.parse(query);
    }

    private SampleFileIndexQuery parseFileQuery(Query query, String sample, Function<String, Collection<String>> filesFromSample) {
        return sampleIndexQueryParser.parseFileQuery(query, sample, filesFromSample, false);
    }

    private byte parseAnnotationMask(Query query) {
        return sampleIndexQueryParser.parseAnnotationIndexQuery(query).getAnnotationIndexMask();
    }

    private byte parseAnnotationMask(Query query, boolean completeIndex) {
        return sampleIndexQueryParser.parseAnnotationIndexQuery(query, completeIndex).getAnnotationIndexMask();
    }

    private SampleAnnotationIndexQuery parseAnnotationIndexQuery(Query query) {
        return sampleIndexQueryParser.parseAnnotationIndexQuery(query);
    }

    private SampleAnnotationIndexQuery parseAnnotationIndexQuery(Query query, boolean completeIndex) {
        return sampleIndexQueryParser.parseAnnotationIndexQuery(query, completeIndex);
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
        sampleIndexQuery = parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "INDEL,SNV,INSERTION,DELETION,CNV,BREAKEND,MNV").append(SAMPLE.key(), "S1");
        sampleIndexQuery = parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "INDEL,SNV").append(SAMPLE.key(), "S1");
        sampleIndexQuery = parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "SNV").append(SAMPLE.key(), "S1");
        sampleIndexQuery = parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "SNV,SNP").append(SAMPLE.key(), "S1");
        sampleIndexQuery = parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "SNP").append(SAMPLE.key(), "S1");
        sampleIndexQuery = parse(q);
        // Filter by SNV, as it can not filter by SNP
        assertEquals(Collections.singleton(VariantType.SNV), sampleIndexQuery.getVariantTypes());
        assertEquals(VariantType.SNP.name(), q.getString(TYPE.key()));


        q = new Query(TYPE.key(), "MNV").append(SAMPLE.key(), "S1");
        sampleIndexQuery = parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "MNV,MNP").append(SAMPLE.key(), "S1");
        sampleIndexQuery = parse(q);
        assertTrue(CollectionUtils.isEmpty(sampleIndexQuery.getVariantTypes()));
        assertNull(q.get(TYPE.key()));

        q = new Query(TYPE.key(), "MNP").append(SAMPLE.key(), "S1");
        sampleIndexQuery = parse(q);
        // Filter by MNV, as it can not filter by MNP
        assertEquals(Collections.singleton(VariantType.MNV), sampleIndexQuery.getVariantTypes());
        assertEquals(VariantType.MNP.name(), q.getString(TYPE.key()));

    }

    @Test
    public void parseFileFilterTest() {
        testFilter("PASS", true, true);
        testFilter("!PASS", false, true);
        testFilter("LowQual", false, false);
        testFilter("!LowQual", null, false);
        testFilter( "LowGQX,LowQual", false, false);
        testFilter( "LowGQX;LowQual", false, false);
        testFilter( "PASS,LowQual", null, false);
        testFilter( "PASS;LowQual", null, false);
        testFilter("!LowGQX;!LowQual", null, false);
    }

    private void testFilter(String value, Boolean pass, boolean covered) {
        Query query = new Query(FILTER.key(), value);
        SampleFileIndexQuery q = parseFileQuery(query, "", null);
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
        assertEquals(!covered, isValidParam(query, FILTER));
    }

    @Test
    public void parseFileQualTest() {

        int qualThresholdMatches = 0;
        for (Double qual : Arrays.asList(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 40.0)) {
            Query query;
            SampleFileIndexQuery fileQuery;

            query = new Query(QUAL.key(), "=" + qual);
            fileQuery = parseFileQuery(query, "", null);
            assertEquals("=" + qual, new RangeQuery(qual, qual + DELTA,
                            IndexUtils.getRangeCode(qual, SampleIndexConfiguration.QUAL_THRESHOLDS),
                            IndexUtils.getRangeCodeExclusive(qual + DELTA, SampleIndexConfiguration.QUAL_THRESHOLDS)),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());
            assertTrue(isValidParam(query, QUAL));

            query = new Query(QUAL.key(), "<=" + qual);
            fileQuery = parseFileQuery(query, "", null);
            assertEquals("<=" + qual, new RangeQuery(Double.MIN_VALUE, qual + DELTA,
                            (byte) 0,
                            IndexUtils.getRangeCodeExclusive(qual + DELTA, SampleIndexConfiguration.QUAL_THRESHOLDS)),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());
            assertTrue(isValidParam(query, QUAL));

            query = new Query(QUAL.key(), "<" + qual);
            fileQuery = parseFileQuery(query, "", null);
            assertEquals("<" + qual, new RangeQuery(Double.MIN_VALUE, qual,
                            (byte) 0,
                            IndexUtils.getRangeCodeExclusive(qual, SampleIndexConfiguration.QUAL_THRESHOLDS)),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());
            if (Arrays.binarySearch(SampleIndexConfiguration.QUAL_THRESHOLDS, qual) >= 0) {
                assertFalse(isValidParam(query, QUAL));
                qualThresholdMatches++;
            } else {
                assertTrue(isValidParam(query, QUAL));
            }

            query = new Query(QUAL.key(), ">=" + qual);
            fileQuery = parseFileQuery(query, "", null);
            assertEquals(">=" + qual, new RangeQuery(qual, IndexUtils.MAX,
                            IndexUtils.getRangeCode(qual, SampleIndexConfiguration.QUAL_THRESHOLDS),
                            (byte) 4),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());
            if (qual == 0 || Arrays.binarySearch(SampleIndexConfiguration.QUAL_THRESHOLDS, qual) >= 0) {
                assertFalse(isValidParam(query, QUAL));
            } else {
                assertTrue(isValidParam(query, QUAL));
            }

            query = new Query(QUAL.key(), ">" + qual);
            fileQuery = parseFileQuery(query, "", null);
            assertEquals(">" + qual, new RangeQuery(qual + DELTA, IndexUtils.MAX,
                            IndexUtils.getRangeCode(qual + DELTA, SampleIndexConfiguration.QUAL_THRESHOLDS),
                            (byte) 4),
                    fileQuery.getQualQuery());
            assertEquals(VariantFileIndexConverter.QUAL_MASK, fileQuery.getFileIndexMask());
            assertTrue(isValidParam(query, QUAL));
        }
        assertEquals(SampleIndexConfiguration.QUAL_THRESHOLDS.length, qualThresholdMatches);
    }

    @Test
    public void parseFileDPTest() {
        SampleFileIndexQuery fileQuery;
        for (Double dp : Arrays.asList(3.0, 5.0, 10.0, 15.0, 20.0, 30.0, 35.0)) {
            for (Pair<String, String> pair : Arrays.asList(Pair.of(INFO.key(), "F1"), Pair.of(FORMAT.key(), "S1"))) {
                fileQuery = parseFileQuery(new Query(pair.getKey(), pair.getValue() + ":DP=" + dp), "S1", n -> Collections.singleton("F1"));
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
                assertEquals(">=" + dp, new RangeQuery(dp, IndexUtils.MAX,
                                IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS),
                                (byte) 4),
                        fileQuery.getDpQuery());
                assertEquals(VariantFileIndexConverter.DP_MASK, fileQuery.getFileIndexMask());

                fileQuery = parseFileQuery(new Query(pair.getKey(), pair.getValue() + ":DP>" + dp), "S1", n -> Collections.singleton("F1"));
                assertEquals(">" + dp, new RangeQuery(dp + DELTA, IndexUtils.MAX,
                                IndexUtils.getRangeCode(dp + DELTA, SampleIndexConfiguration.DP_THRESHOLDS),
                                (byte) 4),
                        fileQuery.getDpQuery());
                assertEquals(VariantFileIndexConverter.DP_MASK, fileQuery.getFileIndexMask());
            }
        }


        Query query = new Query(FORMAT.key(), "S1:DP>=15");
        fileQuery = parseFileQuery(query, "S1", n -> Collections.singleton("F1"));
        assertTrue(fileQuery.getDpQuery().isExactQuery());
        assertFalse(isValidParam(query, FORMAT));

        query = new Query(FORMAT.key(), "S1:DP>=15;S2:DP>34");
        fileQuery = parseFileQuery(query, "S1", n -> Collections.singleton("F1"));
        assertTrue(fileQuery.getDpQuery().isExactQuery());
        fileQuery = parseFileQuery(query, "S2", n -> Collections.singleton("F1"));
        assertFalse(fileQuery.getDpQuery().isExactQuery());
        assertEquals("S2:DP>34", query.getString(FORMAT.key()));

        query = new Query(FORMAT.key(), "S1:DP>=15,S2:DP>34");
        fileQuery = parseFileQuery(query, "S1", n -> Collections.singleton("F1"));
        assertNull(fileQuery.getDpQuery());
        fileQuery = parseFileQuery(query, "S2", n -> Collections.singleton("F1"));
        assertNull(fileQuery.getDpQuery());
        assertEquals("S1:DP>=15,S2:DP>34", query.getString(FORMAT.key()));

        query = new Query(FORMAT.key(), "S2:DP>34;S1:DP>=15;S3:DP>16");
        fileQuery = parseFileQuery(query, "S2", n -> Collections.singleton("F1"));
        assertFalse(fileQuery.getDpQuery().isExactQuery());
        fileQuery = parseFileQuery(query, "S1", n -> Collections.singleton("F1"));
        assertTrue(fileQuery.getDpQuery().isExactQuery());
        assertEquals("S2:DP>34;S3:DP>16", query.getString(FORMAT.key()));
    }

    @Test
    public void parseFamilyQuery() {
        Query query;
        SampleIndexQuery indexQuery;

        query = new Query(SAMPLE.key(), "fam1_child;fam1_father;fam1_mother");
        indexQuery = parse(query);
        assertEquals(Collections.singleton("fam1_child"), indexQuery.getSamplesMap().keySet());
        assertEquals(1, indexQuery.getFatherFilterMap().size());

        query = new Query(GENOTYPE.key(), "fam1_child:0/1;fam1_father:0/1;fam1_mother:0/1,0/0");
        indexQuery = parse(query);
        assertEquals(Collections.singleton("fam1_child"), indexQuery.getSamplesMap().keySet());
        assertEquals(1, indexQuery.getFatherFilterMap().size());

        // Can not use family query with OR operator
        query = new Query(SAMPLE.key(), "fam1_child,fam1_father,fam1_mother");
        indexQuery = parse(query);
        assertEquals(3, indexQuery.getSamplesMap().size());

        // Can not use family query with non valid genotypes in the child
        query = new Query(GENOTYPE.key(), "fam1_child:0/0;fam1_father:0/1;fam1_mother:0/1");
        indexQuery = parse(query);
        assertEquals(3, indexQuery.getSamplesMap().size());
    }

    @Test
    public void parseFamilyQuery_filter() {
        Query query;
        SampleIndexQuery indexQuery;

        query = new Query(SAMPLE.key(), "fam1_child;fam1_father;fam1_mother").append(FILTER.key(), "PASS");
        indexQuery = parse(query);
        assertEquals(Collections.singleton("fam1_child"), indexQuery.getSamplesMap().keySet());
        assertEquals(1, indexQuery.getFatherFilterMap().size());
        assertFalse(query.containsKey(FILTER.key()));

        // Samples from family2 are not in the same file, so we can not remove the FILTER parameter
        query = new Query(SAMPLE.key(), "fam2_child;fam2_father;fam2_mother").append(FILTER.key(), "PASS");
        indexQuery = parse(query);
        assertEquals(Collections.singleton("fam2_child"), indexQuery.getSamplesMap().keySet());
        assertEquals(1, indexQuery.getFatherFilterMap().size());
        assertTrue(query.containsKey(FILTER.key()));
    }

    @Test
    public void parseFamilyQuery_dp() {
        Query query;
        SampleIndexQuery indexQuery;

        query = new Query()
                .append(SAMPLE.key(), "fam1_child;fam1_father;fam1_mother")
                .append(FORMAT.key(), "fam1_father:DP>15;fam1_child:DP>=15;fam1_mother:DP>15");
        indexQuery = parse(query);

        assertEquals(Collections.singleton("fam1_child"), indexQuery.getSamplesMap().keySet());
        assertEquals(1, indexQuery.getFatherFilterMap().size());
        assertTrue(indexQuery.getSampleFileIndexQuery("fam1_child").getDpQuery().isExactQuery());
        assertEquals("fam1_father:DP>15;fam1_mother:DP>15", query.getString(FORMAT.key()));

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

            final double d = DELTA * 10;

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
    public void testGetRangeQuery() {
        RangeQuery rangeQuery;

        double[] thresholds = {1, 2, 3};
        double[] thresholds2 = {0.5, 1.5, 2.5};
        for (int i = 0; i <= thresholds.length; i++) {
            rangeQuery = sampleIndexQueryParser.getRangeQuery(">", i, thresholds, 0, 100);
            assertEquals(i, rangeQuery.getMinCodeInclusive());
            assertEquals(4, rangeQuery.getMaxCodeExclusive());
            assertEquals(false, rangeQuery.isExactQuery());

            rangeQuery = sampleIndexQueryParser.getRangeQuery(">=", i, thresholds, 0, 100);
            assertEquals(i, rangeQuery.getMinCodeInclusive());
            assertEquals(4, rangeQuery.getMaxCodeExclusive());
            assertEquals(true, rangeQuery.isExactQuery());

            rangeQuery = sampleIndexQueryParser.getRangeQuery(">=", i, thresholds2, 0, 100);
            assertEquals(i, rangeQuery.getMinCodeInclusive());
            assertEquals(4, rangeQuery.getMaxCodeExclusive());
            assertEquals(i == 0, rangeQuery.isExactQuery());

            rangeQuery = sampleIndexQueryParser.getRangeQuery(">=", i, thresholds2, -1, 100);
            assertEquals(i, rangeQuery.getMinCodeInclusive());
            assertEquals(4, rangeQuery.getMaxCodeExclusive());
            assertEquals(false, rangeQuery.isExactQuery());

            rangeQuery = sampleIndexQueryParser.getRangeQuery("<", i, thresholds, 0, 100);
            assertEquals(0, rangeQuery.getMinCodeInclusive());
            assertEquals(Math.max(i, 1), rangeQuery.getMaxCodeExclusive());
            assertEquals(i != 0, rangeQuery.isExactQuery());

            rangeQuery = sampleIndexQueryParser.getRangeQuery("<", i, thresholds2, 0, 100);
            assertEquals(0, rangeQuery.getMinCodeInclusive());
            assertEquals(i + 1, rangeQuery.getMaxCodeExclusive());
            assertEquals(false, rangeQuery.isExactQuery());

            rangeQuery = sampleIndexQueryParser.getRangeQuery("<", i, thresholds2, 0, 3);
            assertEquals(0, rangeQuery.getMinCodeInclusive());
            assertEquals(i + 1, rangeQuery.getMaxCodeExclusive());
            assertEquals(i == 3, rangeQuery.isExactQuery());

            rangeQuery = sampleIndexQueryParser.getRangeQuery("<=", i, thresholds, 0, 100);
            assertEquals(0, rangeQuery.getMinCodeInclusive());
            assertEquals(i + 1, rangeQuery.getMaxCodeExclusive());
            assertEquals(false, rangeQuery.isExactQuery());
        }

        rangeQuery = sampleIndexQueryParser.getRangeQuery("<", 100, thresholds, 0, 100);
        assertEquals(0, rangeQuery.getMinCodeInclusive());
        assertEquals(4, rangeQuery.getMaxCodeExclusive());
        assertEquals(true, rangeQuery.isExactQuery());

        rangeQuery = sampleIndexQueryParser.getRangeQuery("<", 50, thresholds, 0, 100);
        assertEquals(0, rangeQuery.getMinCodeInclusive());
        assertEquals(4, rangeQuery.getMaxCodeExclusive());
        assertEquals(false, rangeQuery.isExactQuery());

        rangeQuery = sampleIndexQueryParser.getRangeQuery(">=", -100, thresholds, -100, 100);
        assertEquals(0, rangeQuery.getMinCodeInclusive());
        assertEquals(4, rangeQuery.getMaxCodeExclusive());
        assertEquals(true, rangeQuery.isExactQuery());

        rangeQuery = sampleIndexQueryParser.getRangeQuery(">=", -50, thresholds, -100, 100);
        assertEquals(0, rangeQuery.getMinCodeInclusive());
        assertEquals(4, rangeQuery.getMaxCodeExclusive());
        assertEquals(false, rangeQuery.isExactQuery());
    }

    @Test
    public void testCoveredQuery_ct() {
        Query query;
        SampleAnnotationIndexQuery indexQuery;

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");
        parseAnnotationIndexQuery(query, true);
        assertFalse(query.isEmpty());

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_SET));
        parseAnnotationIndexQuery(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET));
        parseAnnotationIndexQuery(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET));
        parseAnnotationIndexQuery(query, false);
        assertFalse(query.isEmpty()); // Not all samples annotated

        // Use CT column
        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantAnnotationUtils.STOP_LOST));
        parseAnnotationIndexQuery(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(2, 4)));
        parseAnnotationIndexQuery(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantAnnotationUtils.STOP_LOST));
        parseAnnotationIndexQuery(query, false);
        indexQuery = parseAnnotationIndexQuery(query, false);
        assertNotEquals(EMPTY_MASK, indexQuery.getConsequenceTypeMask());
        assertFalse(query.isEmpty()); // Index not complete

        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantAnnotationUtils.MATURE_MIRNA_VARIANT));
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertNotEquals(EMPTY_MASK, indexQuery.getConsequenceTypeMask());
        assertFalse(query.isEmpty()); // Imprecise CT value

    }


    @Test
    public void testCoveredQuery_biotype() {
        Query query;
        SampleAnnotationIndexQuery indexQuery;

        query = new Query().append(ANNOT_BIOTYPE.key(), VariantAnnotationUtils.PROTEIN_CODING);
        parseAnnotationIndexQuery(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_BIOTYPE.key(), VariantAnnotationUtils.PROTEIN_CODING + "," + VariantAnnotationUtils.MIRNA);
        parseAnnotationIndexQuery(query, true);
        assertTrue(query.isEmpty());

        query = new Query().append(ANNOT_BIOTYPE.key(), VariantAnnotationUtils.PROTEIN_CODING + "," + VariantAnnotationUtils.MIRNA);
        parseAnnotationIndexQuery(query, false);
        assertFalse(query.isEmpty()); // Index not complete

        query = new Query().append(ANNOT_BIOTYPE.key(), VariantAnnotationUtils.LINCRNA);
        parseAnnotationIndexQuery(query, true);
        assertFalse(query.isEmpty()); // Imprecise BT value
    }

    @Test
    public void testCoveredQuery_popFreq() {
        Query query;
        SampleAnnotationIndexQuery indexQuery;

        // Query fully covered by summary index.
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), String.join(OR, new ArrayList<>(AnnotationIndexConverter.POP_FREQ_ANY_001_FILTERS)));
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(0, indexQuery.getPopulationFrequencyQueries().size());
        assertTrue(query.isEmpty());

        // Partial summary usage. Also use PopFreqIndex. Clear query
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "GNOMAD_GENOMES:ALL<" + POP_FREQ_THRESHOLD_001);
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(1, indexQuery.getPopulationFrequencyQueries().size());
        assertTrue(query.isEmpty());

        // Partial summary usage, filter more restrictive. Also use PopFreqIndex. Clear query
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "GNOMAD_GENOMES:ALL<" + POP_FREQ_THRESHOLD_001 / 2);
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(1, indexQuery.getPopulationFrequencyQueries().size());
        assertFalse(query.isEmpty());

        // Summary filter less restrictive. Do not use summary. Only use PopFreqIndex. Do not clear query
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "GNOMAD_GENOMES:ALL<" + POP_FREQ_THRESHOLD_001 * 2);
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(1, indexQuery.getPopulationFrequencyQueries().size());
        assertFalse(query.isEmpty());

        // Summary index query plus a new filter. Use only popFreqIndex
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), String.join(OR, new ArrayList<>(AnnotationIndexConverter.POP_FREQ_ANY_001_FILTERS)) + OR + "s1:ALL<0.005");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(QueryOperation.OR, indexQuery.getPopulationFrequencyQueryOperator());
        assertEquals(3, indexQuery.getPopulationFrequencyQueries().size());
        assertTrue(query.isEmpty());

        // Summary index query with AND instead of OR filter. Use both, summary and popFreqIndex
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), String.join(AND, new ArrayList<>(AnnotationIndexConverter.POP_FREQ_ANY_001_FILTERS)));
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(QueryOperation.AND, indexQuery.getPopulationFrequencyQueryOperator());
        assertEquals(2, indexQuery.getPopulationFrequencyQueries().size());
        assertTrue(query.isEmpty());

        // Summary index query with AND instead of OR filter plus a new filter. Use both, summary and popFreqIndex. Leave eextra filter in query
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), String.join(AND, new ArrayList<>(AnnotationIndexConverter.POP_FREQ_ANY_001_FILTERS)) + AND + "s1:ALL<0.05");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(QueryOperation.AND, indexQuery.getPopulationFrequencyQueryOperator());
        assertEquals(3, indexQuery.getPopulationFrequencyQueries().size());
        assertEquals("s1:ALL<0.05", query.getString(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key()));

        // Summary index query with AND instead of OR filter plus a new filter. Use both, summary and popFreqIndex. Clear covered query
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), String.join(AND, new ArrayList<>(AnnotationIndexConverter.POP_FREQ_ANY_001_FILTERS)) + AND + "s1:ALL<0.005");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(QueryOperation.AND, indexQuery.getPopulationFrequencyQueryOperator());
        assertEquals(3, indexQuery.getPopulationFrequencyQueries().size());
        assertTrue(query.isEmpty());

        // Intersect (AND) with an extra study not in index. Don't use summary, PopFreqIndex, and leave other filters in the query
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "s1:ALL<" + POP_FREQ_THRESHOLD_001 + AND + "OtherStudy:ALL<0.8");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(QueryOperation.AND, indexQuery.getPopulationFrequencyQueryOperator());
        assertEquals(1, indexQuery.getPopulationFrequencyQueries().size());
        assertEquals("s1:ALL", indexQuery.getPopulationFrequencyQueries().get(0).getStudyPopulation());
        assertEquals("OtherStudy:ALL<0.8", query.getString(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key()));

        // Intersect (AND) with an extra study not in index. Use summary , PopFreqIndex, and leave other filters in the query
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "GNOMAD_GENOMES:ALL<" + POP_FREQ_THRESHOLD_001 + AND + "OtherStudy:ALL<0.8");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(POP_FREQ_ANY_001_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(QueryOperation.AND, indexQuery.getPopulationFrequencyQueryOperator());
        assertEquals(1, indexQuery.getPopulationFrequencyQueries().size());
        assertEquals("GNOMAD_GENOMES:ALL", indexQuery.getPopulationFrequencyQueries().get(0).getStudyPopulation());
        assertEquals("OtherStudy:ALL<0.8", query.getString(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key()));

        // Union (OR) with an extra study not in index. Do not use index at all
        query = new Query().append(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "GNOMAD_GENOMES:ALL<" + POP_FREQ_THRESHOLD_001 + OR + "OtherStudy:ALL<0.8");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndexMask() & POP_FREQ_ANY_001_MASK);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndex() & POP_FREQ_ANY_001_MASK);
        assertEquals(QueryOperation.OR, indexQuery.getPopulationFrequencyQueryOperator());
        assertEquals(0, indexQuery.getPopulationFrequencyQueries().size());
        assertEquals("GNOMAD_GENOMES:ALL<" + POP_FREQ_THRESHOLD_001 + OR + "OtherStudy:ALL<0.8", query.getString(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key()));
    }

    @Test
    public void testCoveredQuery_combined() {
        Query query;
        SampleAnnotationIndexQuery indexQuery;

        //  LoFE + gene -> Use LOFE mask. Do not clear query, as it's using genes
        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET))
                .append(GENE.key(), "BRCA2");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndex() & LOFE_PROTEIN_CODING_MASK);
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_CONSEQUENCE_TYPE));
        assertTrue(VariantQueryUtils.isValidParam(query, GENE));

        // LoFE + protein_coding -> Use summary mask. Fully covered (clear query)
        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET))
                .append(ANNOT_BIOTYPE.key(), "protein_coding");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(LOFE_PROTEIN_CODING_MASK, indexQuery.getAnnotationIndex() & LOFE_PROTEIN_CODING_MASK);
        assertFalse(VariantQueryUtils.isValidParam(query, ANNOT_CONSEQUENCE_TYPE));
        assertFalse(VariantQueryUtils.isValidParam(query, ANNOT_BIOTYPE));

        //  LoFE + protein_coding + gene -> Use summary mask. Do not clear query, as it's using genes
        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET))
                .append(ANNOT_BIOTYPE.key(), "protein_coding")
                .append(GENE.key(), "BRCA2");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(LOFE_PROTEIN_CODING_MASK, indexQuery.getAnnotationIndex() & LOFE_PROTEIN_CODING_MASK);
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_CONSEQUENCE_TYPE));
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_BIOTYPE));
        assertTrue(VariantQueryUtils.isValidParam(query, GENE));

        // LoFE subset + protein_coding -> Use summary mask. Not fully covered (not clear query)
        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(0, 5)))
                .append(ANNOT_BIOTYPE.key(), "protein_coding");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(LOFE_PROTEIN_CODING_MASK, indexQuery.getAnnotationIndex() & LOFE_PROTEIN_CODING_MASK);
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_CONSEQUENCE_TYPE));
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_BIOTYPE));

        // LoFE + protein_coding + others -> Can not use summary mask
        query = new Query().append(ANNOT_CONSEQUENCE_TYPE.key(), String.join(OR, VariantQueryUtils.LOF_EXTENDED_SET))
                .append(ANNOT_BIOTYPE.key(), "protein_coding,miRNA");
        indexQuery = parseAnnotationIndexQuery(query, true);
        assertEquals(EMPTY_MASK, indexQuery.getAnnotationIndex() & LOFE_PROTEIN_CODING_MASK);
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_CONSEQUENCE_TYPE));
        assertTrue(VariantQueryUtils.isValidParam(query, ANNOT_BIOTYPE));

    }
}