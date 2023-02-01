package org.opencb.opencga.storage.benchmark;

import org.junit.Before;
import org.opencb.opencga.storage.benchmark.variant.generators.FixedQueryGenerator;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wasim on 31/10/18.
 */
public class BenchmarkRunnerTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    private BenchmarkRunner benchmarkRunner;

    @Before
    public void setup() throws Exception {
        runDefaultETL(smallInputUri, getVariantStorageEngine(), newStudyMetadata());

        Map<String, String> params = new HashMap<>();
        params.put(FixedQueryGenerator.DATA_DIR, "src/test/resources/hsapiens");

        benchmarkRunner = new BenchmarkRunner(getVariantStorageEngine().getConfiguration(),
                null,
                Paths.get(newOutputUri()));
    }

}
