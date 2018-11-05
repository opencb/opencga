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
        runDefaultETL(smallInputUri, getVariantStorageEngine(), newStudyConfiguration());

        Map<String, String> params = new HashMap<>();
        params.put(FixedQueryGenerator.DATA_DIR, "src/test/resources/hsapiens");

    }

    @Test
    public void testByRegionAndProteinSubstitution() throws Exception {
        queriesTestDefault("Region,protein-substitution(2)", BenchmarkRunner.ExecutionMode.FIXED, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByRegionDirectAndRandom() throws Exception {
        queriesTestDefault("region", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByGeneDirectAndRandom() throws Exception {
        queriesTestDefault("gene", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByCTDirectAndRandom() throws Exception {
        queriesTestDefault("ct", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByTypeDirectAndRandom() throws Exception {
        queriesTestDefault("type", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByStudyDirectAndRandom() throws Exception {
        queriesTestDefault("study", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByBioTypeDirectAndRandom() throws Exception {
        queriesTestDefault("biotype", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByxrefsDirectAndRandom() throws Exception {
        queriesTestDefault("xrefs", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByfileDirectAndRandom() throws Exception {
        queriesTestDefault("file", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testBysampleDirectAndRandom() throws Exception {
        queriesTestDefault("sample", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByFilterDirectAndRandom() throws Exception {
        queriesTestDefault("filter", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByDrugDirectAndRandom() throws Exception {
        queriesTestDefault("drug", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByClinicalSignificanceDirectAndRandom() throws Exception {
        queriesTestDefault("clinicalSignificance", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByTranscriptionFlagDirectAndRandom() throws Exception {
        queriesTestDefault("transcriptionFlag", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByConservationDirectAndRandom() throws Exception {
        queriesTestDefault("conservation", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByProteinSubstitutionDirectAndRandom() throws Exception {
        queriesTestDefault("proteinSubstitution", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByPopulationFrequencyAltDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyAlt", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByPopulationFrequencyRefDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyRef", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByPopulationFrequencyMafDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyMaf", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByFunctionalScoreMafDirectAndRandom() throws Exception {
        queriesTestDefault("functionalScore", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByRegionAndBiotypeDirectAndFixed() throws Exception {
        queriesTestDefault("RegionAndBiotype", BenchmarkRunner.ExecutionMode.FIXED, BenchmarkRunner.ConnectionType.DIRECT);
    }

    @Test
    public void testByGeneDirectAndFixed() throws Exception {
        queriesTestDefault("Gene", BenchmarkRunner.ExecutionMode.FIXED, BenchmarkRunner.ConnectionType.DIRECT);
    }


    @Test
    public void testByGeneDirec22tAndFixed() throws Exception {
        queriesTestDefault("Gene", BenchmarkRunner.ExecutionMode.FIXED, BenchmarkRunner.ConnectionType.REST);
    }

    @Test
    public void testByRegionAndBiotypeAndGeneDirectAndFixed() throws Exception {
        queriesTestDefault("RegionAndBiotype,Gene", BenchmarkRunner.ExecutionMode.FIXED, BenchmarkRunner.ConnectionType.DIRECT);
    }


    public void queriesTestDefault(String query, BenchmarkRunner.ExecutionMode mode, BenchmarkRunner.ConnectionType connectionType) throws Exception {

        List<Integer> totalQueryExecutions = setTestConfig();
        variantBenchmarkRunner.addThreadGroup(connectionType, mode,
                Paths.get("src/test/resources/hsapiens"), query, new QueryOptions());
        variantBenchmarkRunner.run();
        assertEquals(executedQueries(), totalQueryExecutions.get(0) * totalQueryExecutions.get(1));
        assertEquals(grepFile(), totalQueryExecutions.get(0) * totalQueryExecutions.get(1));
    }


    private long executedQueries() throws IOException {
        return Files.lines(Paths.get(outdir.getPath(), "opencga.benchmark.jtl")).count() - 1;
    }

    private List<Integer> setTestConfig() {
        int concurrency = 1 + random.nextInt(1);
        int repetition = 1 + random.nextInt(3);
        variantBenchmarkRunner.storageConfiguration.getBenchmark().setConcurrency(concurrency);
        variantBenchmarkRunner.storageConfiguration.getBenchmark().setNumRepetitions(repetition);
        return Arrays.asList(concurrency, repetition);
    }

    private int grepFile() throws IOException {
//        DataInputStream input = new DataInputStream(IOUtils.grepFile(Paths.get(outdir.getPath().toString() + "opencga.benchmark.jtl"), "true", false, true));
        Profile localProfile = ProfileBuilder.newBuilder().
                name("test").filePath(outdir.getPath() + "opencga.benchmark.jtl").
                onLocalhost().build();
        GrepResults results
                = Grep4j.grep(Grep4j.constantExpression("true"), localProfile);
        return results.totalLines();

    }
}
