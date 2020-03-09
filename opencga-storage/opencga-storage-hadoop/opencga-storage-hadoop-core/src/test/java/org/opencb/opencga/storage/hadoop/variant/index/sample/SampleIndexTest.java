package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.SampleIndexAnnotationLoaderDriver;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static org.junit.Assert.*;
import static org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils.THREE_PRIME_UTR_VARIANT;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 12/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    private VariantHadoopDBAdaptor dbAdaptor;
    private static boolean loaded = false;
    protected static final List<String> studies = Arrays.asList(STUDY_NAME, STUDY_NAME_2);
    protected static final List<String> sampleNames = Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685");

    @Before
    public void before() throws Exception {
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        if (!loaded) {
            load();
            loaded = true;
        }
    }

    public void load() throws Exception {
        clearDB(DB_NAME);

        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), CellBaseRestVariantAnnotator.class.getName())
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);


        runETL(getVariantStorageEngine(), smallInputUri, outputUri, params, true, true, true);
        params.append(VariantStorageOptions.STUDY.key(), STUDY_NAME_2);
        params.append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.LoadSplitData.MULTI);

        runETL(getVariantStorageEngine(), getResourceUri("by_chr/chr22_1-1.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(getVariantStorageEngine(), getResourceUri("by_chr/chr22_1-2.variant-test-file.vcf.gz"), outputUri, params, true, true, true);
        runETL(getVariantStorageEngine(), getResourceUri("by_chr/chr22_1-2-DUP.variant-test-file.vcf.gz"), outputUri, params, true, true, true);

        variantStorageEngine.annotate(new Query(), new QueryOptions());

        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
    }

    @Test
    public void checkLoadedData() throws Exception {
        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        Iterator<SampleMetadata> it = variantStorageEngine.getMetadataManager().sampleMetadataIterator(1);
        while (it.hasNext()) {
            SampleMetadata sample = it.next();
            Iterator<SampleIndexEntry> indexIt = variantStorageEngine.getSampleIndexDBAdaptor().rawIterator(1, sample.getId());
            while (indexIt.hasNext()) {
                SampleIndexEntry record = indexIt.next();

                List<Variant> variants = variantStorageEngine.getDBAdaptor().get(new Query()
                        .append(VariantQueryParam.SAMPLE.key(), sample.getName())
                        .append(VariantQueryParam.REGION.key(),
                                record.getChromosome() + ":" + record.getBatchStart() + "-" + (record.getBatchStart() + SampleIndexSchema.BATCH_SIZE - 1)), null).getResults();

                Map<String, List<String>> gtsMap = variants.stream().collect(groupingBy(v -> v.getStudies().get(0).getSampleData(0).get(0), mapping(Variant::toString, toList())));
//                System.out.println("record = " + record);

                Assert.assertEquals(gtsMap.keySet(), record.getGts().keySet());
                for (Map.Entry<String, SampleIndexEntry.SampleIndexGtEntry> entry : record.getGts().entrySet()) {
                    String gt = entry.getKey();
                    List<String> expectedVariants = gtsMap.get(gt);
                    List<String> actualVariants;
                    if (entry.getValue().getVariants() == null) {
                        actualVariants = Collections.emptyList();
                    } else {
                        actualVariants = Lists.newArrayList(entry.getValue().iterator())
                                .stream()
                                .map(Variant::toString)
                                .collect(toList());
                    }
                    Assert.assertEquals(gt, expectedVariants, actualVariants);
                }

            }
        }

    }

    @Test
    public void regenerateSampleIndex() throws Exception {

        String orig = dbAdaptor.getTableNameGenerator().getSampleIndexTableName(1);
        String copy = orig + "_copy";

        dbAdaptor.getHBaseManager().createTableIfNeeded(copy, Bytes.toBytes(GenomeHelper.COLUMN_FAMILY),
                Compression.Algorithm.NONE);

        ObjectMap options = new ObjectMap()
                .append(SampleIndexDriver.OUTPUT, copy)
                .append(SampleIndexDriver.SAMPLES, "all");
        new TestMRExecutor().run(SampleIndexDriver.class, SampleIndexDriver.buildArgs(
                dbAdaptor.getArchiveTableName(1),
                dbAdaptor.getVariantTable(),
                1,
                Collections.emptySet(), options), options);

        new TestMRExecutor().run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
                dbAdaptor.getArchiveTableName(1),
                dbAdaptor.getVariantTable(),
                1,
                Collections.emptySet(), options), options);


        Connection c = dbAdaptor.getHBaseManager().getConnection();

        ResultScanner origScanner = c.getTable(TableName.valueOf(orig)).getScanner(new Scan());
        ResultScanner copyScanner = c.getTable(TableName.valueOf(copy)).getScanner(new Scan());
        while (true) {
            Result origValue = origScanner.next();
            Result copyValue = copyScanner.next();
            if (origValue == null) {
                assertNull(copyValue);
                break;
            }
            NavigableMap<byte[], byte[]> origFamily = origValue.getFamilyMap(GenomeHelper.COLUMN_FAMILY_BYTES);
            NavigableMap<byte[], byte[]> copyFamily = copyValue.getFamilyMap(GenomeHelper.COLUMN_FAMILY_BYTES);

            assertEquals(origFamily.keySet().stream().map(Bytes::toString).collect(toList()), copyFamily.keySet().stream().map(Bytes::toString).collect(toList()));
            assertEquals(origFamily.size(), copyFamily.size());

            for (byte[] key : origFamily.keySet()) {
                assertArrayEquals(Bytes.toString(key), origFamily.get(key), copyFamily.get(key));
            }
        }



//        VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), copy);

    }

    public VariantQueryResult<Variant> dbAdaptorQuery(Query query, QueryOptions options) {
        query = variantStorageEngine.preProcessQuery(query, options);
        return dbAdaptor.get(query, options);
    }

    @Test
    public void testQueryFileIndex() throws Exception {
        testQueryFileIndex(new Query(TYPE.key(), "SNV"));
        testQueryFileIndex(new Query(TYPE.key(), "SNP"));
        testQueryFileIndex(new Query(TYPE.key(), "INDEL"));
        testQueryFileIndex(new Query(TYPE.key(), "SNV,INDEL"));
        testQueryFileIndex(new Query(FILTER.key(), "PASS"));
        testQueryFileIndex(new Query(TYPE.key(), "INDEL").append(FILTER.key(), "PASS"));
        testQueryFileIndex(new Query(QUAL.key(), ">=30").append(FILTER.key(), "PASS"));
        testQueryFileIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"));
        testQueryIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"), new Query(STUDY.key(), STUDY_NAME).append(SAMPLE.key(), "NA19600,NA19661"));
        testQueryIndex(new Query(QUAL.key(), ">=10").append(FILTER.key(), "PASS"), new Query(STUDY.key(), STUDY_NAME).append(GENOTYPE.key(), "NA19600:0/1;NA19661:0/0"));
    }

    @Test
    public void testQueryAnnotationIndex() throws Exception {
        testQueryAnnotationIndex(new Query(ANNOT_BIOTYPE.key(), "protein_coding"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_lost").append(ANNOT_TRANSCRIPT_FLAG.key(), "basic"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant").append(ANNOT_BIOTYPE.key(), "nonsense_mediated_decay"));
        testQueryAnnotationIndex(new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=tolerated"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL<0.001"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL>0.005"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL>0.008"));
        testQueryAnnotationIndex(new Query(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:ALL>0.005,GNOMAD_GENOMES:ALL>0.005"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "pathogenic"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "likely_benign"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "pathogenic,likely_benign"));
        testQueryAnnotationIndex(new Query(ANNOT_CLINICAL_SIGNIFICANCE.key(), "benign"));

//        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "intergenic_variant"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained"));
        testQueryAnnotationIndex(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,stop_gained,mature_miRNA_variant"));
    }

    public void testQueryAnnotationIndex(Query annotationQuery) throws Exception {
        for (String study : studies) {
            for (String sampleName : sampleNames) {
                SampleIndexQuery sampleIndexQuery = testQueryIndex(annotationQuery, new Query()
                        .append(STUDY.key(), study)
                        .append(SAMPLE.key(), sampleName));
                assertTrue(!sampleIndexQuery.emptyAnnotationIndex() || !sampleIndexQuery.getAnnotationIndexQuery().getPopulationFrequencyQueries().isEmpty());
            }
        }
    }

    public void testQueryFileIndex(Query query) throws Exception {
        for (String study : studies) {
            for (String sampleName : sampleNames) {
                testQueryIndex(query, new Query()
                        .append(STUDY.key(), study)
                        .append(SAMPLE.key(), sampleName));
            }
        }
    }

    public SampleIndexQuery testQueryIndex(Query testQuery, Query query) throws Exception {
        System.out.println("----- START TEST QUERY INDEX -----------------------------------------------------");
        System.out.println("testQuery  = " + testQuery.toJson());
        System.out.println("query      = " + query.toJson());
        System.out.println("--------");

        // Query DBAdaptor
        System.out.println("#Query DBAdaptor");
        query.putAll(testQuery);
        VariantQueryResult<Variant> queryResult = dbAdaptorQuery(new Query(query), new QueryOptions());
        int onlyDBAdaptor = queryResult.getNumResults();

        // Query SampleIndex
        System.out.println("#Query SampleIndex");
        SampleIndexDBAdaptor sampleIndexDBAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor();
        SampleIndexQuery indexQuery = sampleIndexDBAdaptor.getSampleIndexQueryParser().parse(new Query(query));
//        int onlyIndex = (int) ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor()
//                .count(indexQuery, "NA19600");
        int onlyIndex = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor()
                .iterator(indexQuery).toDataResult().getNumResults();

        // Query SampleIndex+DBAdaptor
        System.out.println("#Query SampleIndex+DBAdaptor");
        queryResult = variantStorageEngine.get(new Query(query), new QueryOptions());
        int indexAndDBAdaptor = queryResult.getNumResults();
        System.out.println("queryResult.source = " + queryResult.getSource());

        System.out.println("--- RESULTS -----");
        System.out.println("testQuery  = " + testQuery.toJson());
        System.out.println("query      = " + query.toJson());
        System.out.println("annotationIndex    = " + IndexUtils.maskToString(indexQuery.getAnnotationIndexMask(), indexQuery.getAnnotationIndex()));
        System.out.println("biotypeMask        = " + IndexUtils.byteToString(indexQuery.getAnnotationIndexQuery().getBiotypeMask()));
        System.out.println("ctMask             = " + IndexUtils.shortToString(indexQuery.getAnnotationIndexQuery().getConsequenceTypeMask()));
        System.out.println("clinicalMask       = " + IndexUtils.byteToString(indexQuery.getAnnotationIndexQuery().getClinicalMask()));
//        for (String sample : indexQuery.getSamplesMap().keySet()) {
//            System.out.println("fileIndex("+sample+") = " + IndexUtils.maskToString(indexQuery.getFileIndexMask(sample), indexQuery.getFileIndex(sample)));
//        }
        System.out.println("Query SampleIndex             = " + onlyIndex);
        System.out.println("Query DBAdaptor               = " + onlyDBAdaptor);
        System.out.println("Query SampleIndex+DBAdaptor   = " + indexAndDBAdaptor);
        System.out.println("--------");

        if (onlyDBAdaptor != indexAndDBAdaptor) {
            VariantQueryResult<Variant> queryResultAux = variantStorageEngine.get(new Query(query), new QueryOptions());
            List<String> indexAndDB = queryResultAux.getResults().stream().map(Variant::toString).sorted().collect(Collectors.toList());
            queryResultAux = dbAdaptorQuery(new Query(query), new QueryOptions());
            List<String> noIndex = queryResultAux.getResults().stream().map(Variant::toString).sorted().collect(Collectors.toList());

            for (String s : indexAndDB) {
                if (!noIndex.contains(s)) {
                    System.out.println("From SampleIndex+DB, not in DBAdaptor = " + s);
                }
            }

            for (String s : noIndex) {
                if (!indexAndDB.contains(s)) {
                    System.out.println("From DBAdaptor, not in SampleIndex+DB = " + s);
                }
            }
        }
        assertEquals(onlyDBAdaptor, indexAndDBAdaptor);
        assertThat(queryResult, numResults(lte(onlyIndex)));
        assertThat(queryResult, numResults(gt(0)));
        return indexQuery;
    }

    @Test
    public void testSampleIndexSkipIntersect() throws StorageEngineException {
        Query query = new Query(VariantQueryParam.SAMPLE.key(), sampleNames.get(0)).append(VariantQueryParam.STUDY.key(), STUDY_NAME);
        VariantQueryResult<Variant> result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_SET)));
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET))).append(TYPE.key(), VariantType.INDEL);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(2, 4)));
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(2, 4)))
                .append(ANNOT_BIOTYPE.key(), VariantAnnotationUtils.PROTEIN_CODING);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertEquals("sample_index_table", result.getSource());

        query.append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), String.join(",", new ArrayList<>(VariantQueryUtils.LOF_EXTENDED_SET).subList(2, 4)) + "," + THREE_PRIME_UTR_VARIANT)
                .append(ANNOT_BIOTYPE.key(), VariantAnnotationUtils.PROTEIN_CODING);
        result = variantStorageEngine.get(query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID).append(QueryOptions.LIMIT, 1));
        assertNotEquals("sample_index_table", result.getSource());
    }

    @Test
    public void testCount() throws StorageEngineException {
        List<List<Region>> regionLists = Arrays.asList(null, Arrays.asList(new Region("1", 1000, 16400000)));

        for (List<Region> regions : regionLists) {
            StopWatch stopWatch = StopWatch.createStarted();
            long actualCount = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor()
                    .count(regions, STUDY_NAME, "NA19600", Arrays.asList("1|0", "0|1", "1|1"));
            Query query = new Query(VariantQueryParam.STUDY.key(), STUDY_NAME)
                    .append(VariantQueryParam.SAMPLE.key(), "NA19600");
            if (regions != null) {
                query.append(VariantQueryParam.REGION.key(), regions);
            }
            System.out.println("Count indexTable " + stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000.0);
            System.out.println("Count = " + actualCount);
            stopWatch = StopWatch.createStarted();
            long expectedCount = dbAdaptor.count(query).first();
            System.out.println("Count variants   " + stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000.0);
            System.out.println("Count = " + expectedCount);
            System.out.println("-----------------------------------");
            assertEquals(expectedCount, actualCount);
        }
    }


}