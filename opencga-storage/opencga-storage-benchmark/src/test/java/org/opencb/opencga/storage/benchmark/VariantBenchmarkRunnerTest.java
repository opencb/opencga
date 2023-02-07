package org.opencb.opencga.storage.benchmark;

import org.grep4j.core.Grep4j;
import org.grep4j.core.model.Profile;
import org.grep4j.core.model.ProfileBuilder;
import org.grep4j.core.result.GrepResults;
import org.junit.Before;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.benchmark.variant.VariantBenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.generators.FixedQueryGenerator;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by wasim on 05/11/18.
 */

public class VariantBenchmarkRunnerTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

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
        runDefaultETL(smallInputUri, getVariantStorageEngine(), newStudyMetadata());

        Map<String, String> params = new HashMap<>();
        params.put(FixedQueryGenerator.DATA_DIR, "src/test/resources/hsapiens");
        params.put(QueryOptions.LIMIT, "5");

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
        Profile localProfile = ProfileBuilder.newBuilder().
                name("test").filePath(variantBenchmarkRunner.resultFile).
                onLocalhost().build();
        GrepResults results
                = Grep4j.grep(Grep4j.constantExpression("true"), localProfile);
        return results.totalLines();
    }
}
