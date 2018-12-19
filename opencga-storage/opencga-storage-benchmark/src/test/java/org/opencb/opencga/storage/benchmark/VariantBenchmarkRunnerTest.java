package org.opencb.opencga.storage.benchmark;

import org.grep4j.core.Grep4j;
import org.grep4j.core.model.Profile;
import org.grep4j.core.model.ProfileBuilder;
import org.grep4j.core.result.GrepResults;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.benchmark.variant.VariantBenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.generators.FixedQueryGenerator;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by wasim on 05/11/18.
 */

public class VariantBenchmarkRunnerTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {

    private URI outdir;
    private Random random;
    private VariantBenchmarkRunner variantBenchmarkRunner;
    private static boolean loaded = false;

    @Before
    public void setup() throws Exception {
        if (!loaded) {
            loadFile();
            loaded = true;
        }
        outdir = newOutputUri();
        random = new Random();
        variantBenchmarkRunner = new VariantBenchmarkRunner(getVariantStorageEngine().getConfiguration(),
                Paths.get("src/test/resources/benchmark/jmeter"),
                Paths.get(outdir));

    }

    private void loadFile() throws Exception {
        // runDefaultETL(smallInputUri, getVariantStorageEngine(), newStudyConfiguration());

        Map<String, String> params = new HashMap<>();
        params.put(FixedQueryGenerator.DATA_DIR, "src/test/resources/hsapiens");
        params.put(QueryOptions.LIMIT, "5");

    }

    @Test
    public void testByRegionAndProteinSubstitutionDirectAndFix() throws Exception {
        queriesTestDefault("Region", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionDirectAndRandom() throws Exception {
        queriesTestDefault("region", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByGeneDirectAndRandom() throws Exception {
        queriesTestDefault("gene", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByCTDirectAndRandom() throws Exception {
        queriesTestDefault("ct", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByTypeDirectAndRandom() throws Exception {
        queriesTestDefault("type", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByStudyDirectAndRandom() throws Exception {
        queriesTestDefault("study", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByBioTypeDirectAndRandom() throws Exception {
        queriesTestDefault("biotype", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByxrefsDirectAndRandom() throws Exception {
        queriesTestDefault("xrefs", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByfileDirectAndRandom() throws Exception {
        queriesTestDefault("file", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testBysampleDirectAndRandom() throws Exception {
        queriesTestDefault("sample", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByFilterDirectAndRandom() throws Exception {
        queriesTestDefault("filter", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByDrugDirectAndRandom() throws Exception {
        queriesTestDefault("drug", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByClinicalSignificanceDirectAndRandom() throws Exception {
        queriesTestDefault("clinicalSignificance", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByTranscriptionFlagDirectAndRandom() throws Exception {
        queriesTestDefault("transcriptionFlag", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByConservationDirectAndRandom() throws Exception {
        queriesTestDefault("conservation", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByProteinSubstitutionDirectAndRandom() throws Exception {
        queriesTestDefault("proteinSubstitution", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyAltDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyAlt", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyRefDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyRef", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyMafDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyMaf", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByFunctionalScoreMafDirectAndRandom() throws Exception {
        queriesTestDefault("functionalScore", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByRegionAndBiotypeDirectAndFixed() throws Exception {
        queriesTestDefault("RegionAndBiotype", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByGeneDirectAndFixed() throws Exception {
        queriesTestDefault("Gene", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.FIXED);
    }


    @Test
    public void testByGeneRestAndFixed() throws Exception {
        queriesTestDefault("Gene", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionAndBiotypeAndGeneDirectAndFixed() throws Exception {
        queriesTestDefault("RegionAndBiotype,Gene", 2, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionAndProteinSubstitutionRESTAndFix() throws Exception {
        queriesTestDefault("region,proteinSubstitution(2)", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByRegionRESTAndRandom() throws Exception {
        queriesTestDefault("region", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByGeneRESTAndRandom() throws Exception {
        queriesTestDefault("gene", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByCTRESTAndRandom() throws Exception {
        queriesTestDefault("ct", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByTypeRESTAndRandom() throws Exception {
        queriesTestDefault("type", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

/*
    @Test
    public void testByStudyRESTAndRandom() throws Exception {
        queriesTestDefault("study", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.REST);
    }
*/

    @Test
    public void testByBioTypeRESTAndRandom() throws Exception {
        queriesTestDefault("biotype", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByxrefsRESTAndRandom() throws Exception {
        queriesTestDefault("xrefs", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

/*
    @Test
    public void testByfileRESTAndRandom() throws Exception {
        queriesTestDefault("file", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.REST);
    }


    @Test
    public void testBysampleRESTAndRandom() throws Exception {
        queriesTestDefault("sample", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.REST);
    }
*/

    @Test
    public void testByFilterRESTAndRandom() throws Exception {
        queriesTestDefault("filter", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByDrugRESTAndRandom() throws Exception {
        queriesTestDefault("drug", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByClinicalSignificanceRESTAndRandom() throws Exception {
        queriesTestDefault("clinicalSignificance", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByTranscriptionFlagRESTAndRandom() throws Exception {
        queriesTestDefault("transcriptionFlag", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByConservationRESTAndRandom() throws Exception {
        queriesTestDefault("conservation", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByProteinSubstitutionRESTAndRandom() throws Exception {
        queriesTestDefault("proteinSubstitution", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyAltRESTAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyAlt", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyRefRESTAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyRef", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyMafRESTAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyMaf", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByFunctionalScoreMafRESTAndRandom() throws Exception {
        queriesTestDefault("functionalScore(2)", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByRegionAndBiotypeRESTAndFixed() throws Exception {
        queriesTestDefault("Region", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionAndBiotypeRESTAndRandom() throws Exception {
        queriesTestDefault("region", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByGeneRESTAndFixed() throws Exception {
        queriesTestDefault("Gene", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionAndBiotypeAndGeneRESTAndFixed() throws Exception {
        queriesTestDefault("RegionAndBiotype,Gene", 2, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testAllRESTAndFixed() throws Exception {
        queriesTestDefault("", 4, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    public void queriesTestDefault(String query, int numberOfQueries, BenchmarkRunner.ConnectionType connectionType, BenchmarkRunner.ExecutionMode mode) throws Exception {

        List<Integer> totalQueryExecutions = setTestConfig(connectionType, mode);
        variantBenchmarkRunner.addThreadGroup(connectionType, mode,
                Paths.get("src/test/resources/hsapiens"), new HashMap<>(), "", query, new QueryOptions(QueryOptions.LIMIT, "2"));
        variantBenchmarkRunner.run();

        assertEquals(executedQueries(), numberOfQueries * totalQueryExecutions.get(0) * totalQueryExecutions.get(1));
        assertEquals(grepFile(), numberOfQueries * totalQueryExecutions.get(0) * totalQueryExecutions.get(1));
    }


    private long executedQueries() throws IOException {
        return Files.lines(Paths.get(variantBenchmarkRunner.resultFile)).count() - 1;
    }

    private List<Integer> setTestConfig(BenchmarkRunner.ConnectionType connectionType, BenchmarkRunner.ExecutionMode mode) {
        int concurrency = 1 + random.nextInt(5);
        int repetition = 1 + random.nextInt(5);
        variantBenchmarkRunner.storageConfiguration.getBenchmark().setConcurrency(concurrency);
        variantBenchmarkRunner.storageConfiguration.getBenchmark().setNumRepetitions(repetition);
        variantBenchmarkRunner.storageConfiguration.getBenchmark().setConnectionType(connectionType.toString());
        variantBenchmarkRunner.storageConfiguration.getBenchmark().setMode(mode.toString());
        return Arrays.asList(concurrency, repetition);
    }

    private int grepFile() throws IOException {
//        DataInputStream input = new DataInputStream(IOUtils.grepFile(Paths.get(outdir.getPath() + "opencga.benchmark.jtl"), "true", false, true));
        Profile localProfile = ProfileBuilder.newBuilder().
                name("test").filePath(variantBenchmarkRunner.resultFile).
                onLocalhost().build();
        GrepResults results
                = Grep4j.grep(Grep4j.constantExpression("true"), localProfile);
        return results.totalLines();

    }
}
