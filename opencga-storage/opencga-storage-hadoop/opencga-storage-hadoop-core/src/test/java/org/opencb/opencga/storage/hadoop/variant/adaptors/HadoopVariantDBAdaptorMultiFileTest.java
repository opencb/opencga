package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorMultiFileTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;

/**
 * Created on 24/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantDBAdaptorMultiFileTest extends VariantDBAdaptorMultiFileTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Override
    public void before() throws Exception {
        boolean wasLoaded = loaded;
        super.before();
        if (loaded && !wasLoaded) {
            VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor()), newOutputUri(getClass().getSimpleName()));
//            for (String study : variantStorageEngine.getDBAdaptor().getStudyConfigurationManager().getStudies(null).keySet()) {
//                variantStorageEngine.fillMissing(study, new ObjectMap(), false);
//            }
//            VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) variantStorageEngine.getDBAdaptor()), newOutputUri(getClass().getSimpleName()));
        }
    }

    @Override
    protected ObjectMap getOptions() {
        return new ObjectMap()
                .append(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true)
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
    }


    @Test
    public void testGetBySampleNameHBaseColumnIntersect() throws Exception {
        testGetBySampleName(variantStorageEngine.getStorageEngineId() + " + " + variantStorageEngine.getStorageEngineId(),
                options.append("hbase_column_intersect", true).append("sample_index_intersect", false));
    }

    @Test
    public void testGetBySampleNameSampleIndexIntersect() throws Exception {
        testGetBySampleName(variantStorageEngine.getStorageEngineId() + " + " + "sample_index_table",
                options.append("hbase_column_intersect", false).append("sample_index_intersect", true));
    }

    public void testGetBySampleName(String expectedSource, QueryOptions options) throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.SAMPLE.key(), "NA12877");
//        queryResult = dbAdaptor.get(query, options);
        queryResult = variantStorageEngine.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), options);
        assertEquals(expectedSource, queryResult.getSource());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId("12877"), withSampleData("NA12877", "GT", containsString("1"))))));
    }

    @Test
    public void testGetBySamplesNameHBaseColumnIntersect() throws Exception {
        testGetBySamplesName(variantStorageEngine.getStorageEngineId() + " + " + variantStorageEngine.getStorageEngineId(),
                options.append("hbase_column_intersect", true).append("sample_index_intersect", false));
    }

    @Test
    public void testGetBySamplesNameSampleIndexIntersect() throws Exception {
        testGetBySamplesName(variantStorageEngine.getStorageEngineId() + " + " + "sample_index_table",
                options.append("hbase_column_intersect", false).append("sample_index_intersect", true));
    }

    public void testGetBySamplesName(String expectedSource, QueryOptions options) throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.SAMPLE.key(), "NA12877;NA12878");
//        queryResult = dbAdaptor.get(query, options);
        queryResult = variantStorageEngine.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz"), options);
        assertEquals(expectedSource, queryResult.getSource());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                allOf(
                        withFileId("12877"),
                        withSampleData("NA12877", "GT", containsString("1"))),
                allOf(
                        withFileId("12878"),
                        withSampleData("NA12878", "GT", containsString("1")))))));
    }

    @Test
    @Ignore("Genotypes union filter not supported in HBaseColumn Intersect")
    public void testGetByGenotypesHBaseColumnIntersect() throws Exception {
        testGetByGenotypes(variantStorageEngine.getStorageEngineId() + " + " + variantStorageEngine.getStorageEngineId(),
                options.append("hbase_column_intersect", true).append("sample_index_intersect", false));
    }

    @Test
    public void testGetByGenotypesSampleIndexIntersect() throws Exception {
        testGetByGenotypes(variantStorageEngine.getStorageEngineId() + " + " + "sample_index_table",
                options.append("hbase_column_intersect", false).append("sample_index_intersect", true));
    }

    public void testGetByGenotypes(String expectedSource, QueryOptions options) throws Exception {
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz"), options);

        queryResult = variantStorageEngine.get(new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:0/1,NA12878:1/1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878"), options);

        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", anyOf(
                allOf(
                        withFileId("12877"),
                        withSampleData("NA12877", "GT", containsString("0/1"))),
                allOf(
                        withFileId("12878"),
                        withSampleData("NA12878", "GT", containsString("1/1")))))));
        assertEquals(expectedSource, queryResult.getSource());

        queryResult = variantStorageEngine.get(new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:" + GenotypeClass.HET_REF + ",NA12878:" + GenotypeClass.HOM_ALT)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878"), options);

        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", anyOf(
                allOf(
                        withFileId("12877"),
                        withSampleData("NA12877", "GT", containsString("0/1"))),
                allOf(
                        withFileId("12878"),
                        withSampleData("NA12878", "GT", containsString("1/1")))))));
        assertEquals(expectedSource, queryResult.getSource());
    }

    @Test
    public void testGetByGenotypesWithRefSampleIndexIntersect() throws Exception {
        testGetByGenotypesWithRef(variantStorageEngine.getStorageEngineId() + " + " + "sample_index_table",
                options.append("hbase_column_intersect", false).append("sample_index_intersect", true));
    }

    public void testGetByGenotypesWithRef(String expectedSource, QueryOptions options) throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:0/1;NA12878:0/0")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "0/0");
//        queryResult = dbAdaptor.get(query, options);
        queryResult = variantStorageEngine.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz")
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "0/0"), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                allOf(
                        withFileId("12877"),
                        withSampleData("NA12877", "GT", containsString("0/1"))),
                withSampleData("NA12878", "GT", containsString("0/0"))))));
        assertEquals(expectedSource, queryResult.getSource());
    }

    @Test
    public void testGetByGenotypesWithRefMixSampleIndexIntersect() throws Exception {
        testGetByGenotypesWithRefMix(variantStorageEngine.getStorageEngineId() + " + " + "sample_index_table",
                options.append("hbase_column_intersect", false).append("sample_index_intersect", true));
    }

    public void testGetByGenotypesWithRefMix(String expectedSource, QueryOptions options) throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:0/1;NA12878:0/0,0/1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "0/0");
//        queryResult = dbAdaptor.get(query, options);
        queryResult = variantStorageEngine.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877,NA12878")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz")
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "0/0"), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(
                allOf(
                        withFileId("12877"),
                        withSampleData("NA12877", "GT", containsString("0/1"))),
                anyOf(
                        withSampleData("NA12878", "GT", containsString("0/0")),
                        withSampleData("NA12878", "GT", containsString("0/1"))
                )
        ))));
        assertEquals(expectedSource, queryResult.getSource());
    }

    @Test
    public void testGetBySampleNameMultiRegionHBaseColumnIntersect() throws Exception {
        testGetBySampleNameMultiRegion(variantStorageEngine.getStorageEngineId() + " + " + variantStorageEngine.getStorageEngineId(),
                options.append("hbase_column_intersect", true).append("sample_index_intersect", false));
    }

    @Test
    public void testGetBySampleNameMultiRegionSampleIndexIntersect() throws Exception {
        testGetBySampleNameMultiRegion(variantStorageEngine.getStorageEngineId() + " + " + "sample_index_table",
                options.append("hbase_column_intersect", false).append("sample_index_intersect", true));
    }

    public void testGetBySampleNameMultiRegion(String expectedSource, QueryOptions options) throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.REGION.key(), "1:1-12783,M");
//        queryResult = dbAdaptor.get(query, options);
        queryResult = variantStorageEngine.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), options);
        assertEquals(expectedSource, queryResult.getSource());
        assertThat(queryResult, everyResult(allVariants, allOf(anyOf(overlaps(new Region("1:1-12783")), overlaps(new Region("M"))), withStudy("S_1", allOf(withFileId("12877"), withSampleData("NA12877", "GT", containsString("1")))))));
    }

    @Test
    public void testGetByGenotypeMultiRegionHBaseColumnIntersect() throws Exception {
        testGetByGenotypeMultiRegion(variantStorageEngine.getStorageEngineId() + " + " + variantStorageEngine.getStorageEngineId(),
                options.append("hbase_column_intersect", true).append("sample_index_intersect", false));
    }

    @Test
    public void testGetByGenotypeMultiRegionSampleIndexIntersect() throws Exception {
        testGetByGenotypeMultiRegion(variantStorageEngine.getStorageEngineId() + " + " + "sample_index_table",
                options.append("hbase_column_intersect", false).append("sample_index_intersect", true));
    }

    public void testGetByGenotypeMultiRegion(String expectedSource, QueryOptions options) throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.GENOTYPE.key(), "NA12877:1/1")
                .append(VariantQueryParam.REGION.key(), "1:1-12783,M");
//        queryResult = dbAdaptor.get(query, options);
        queryResult = variantStorageEngine.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "S_1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), options);
        assertEquals(expectedSource, queryResult.getSource());
        assertThat(queryResult, everyResult(allVariants, allOf(anyOf(overlaps(new Region("1:1-12783")), overlaps(new Region("M"))), withStudy("S_1", allOf(withFileId("12877"), withSampleData("NA12877", "GT", is("1/1")))))));
    }

    @Test
    public void testGetByFileNameHBaseColumnIntersect() throws Exception {
        testGetByFileName(variantStorageEngine.getStorageEngineId() + " + " + variantStorageEngine.getStorageEngineId(),
                options.append("hbase_column_intersect", true).append("sample_index_intersect", false));
    }

    @Test
    public void testGetByFileNameSampleIndexIntersect() throws Exception {
        testGetByFileName(variantStorageEngine.getStorageEngineId() + " + " + "sample_index_table",
                options.append("hbase_column_intersect", false).append("sample_index_intersect", true));
    }

    public void testGetByFileName(String expectedSource, QueryOptions options) throws Exception {
        query = new Query()
//                .append(VariantQueryParam.STUDY.key(), "S_1")
                .append(VariantQueryParam.FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
//        queryResult = dbAdaptor.get(query, options);
        queryResult = variantStorageEngine.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), "all")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA12877")
                .append(VariantQueryParam.INCLUDE_FILE.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), options);
        assertEquals(expectedSource, queryResult.getSource());
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId("12877"))));
    }

    @Test
    public void testSampleIndexDBAdaptor() throws StorageEngineException {
        List<List<Region>> regionLists = Arrays.asList(null, Arrays.asList(new Region("1", 1000, 300000)));

        for (List<Region> regions : regionLists) {
            StopWatch stopWatch = StopWatch.createStarted();
            long actualCount = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor().count(regions, "S_1", "NA12877", Arrays.asList("0/1", "1/1"));
            Query query = new Query(VariantQueryParam.STUDY.key(), "S_1")
                    .append(VariantQueryParam.SAMPLE.key(), "NA12877");
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
