package org.opencb.opencga.storage.benchmark.queryGenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.benchmark.BenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.generators.FixedQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.RandomQueries;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wasim on 31/10/18.
 */
public class FixedQueryGeneratorTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {

    private FixedQueryGenerator fixedQueryGenerator;
    private BenchmarkRunner benchmarkRunner;

    @Before
    public void setup() throws Exception {
    /*    runDefaultETL(smallInputUri, getVariantStorageEngine(), newStudyConfiguration());

        Map<String, String> params = new HashMap<>();
        params.put(FixedQueryGenerator.DATA_DIR, "src/test/resources/hsapiens");

        fixedQueryGenerator = new FixedQueryGenerator();
        fixedQueryGenerator.setUp(params);

        benchmarkRunner = new BenchmarkRunner(getVariantStorageEngine().getConfiguration(),
                null,
                Paths.get(newOutputUri()));*/
    }

    @Test
    public void test() throws IOException {
        try (FileInputStream inputStream = new FileInputStream(Paths.get("src/test/resources/hsapiens/randomQueries.yml").toFile());) {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            RandomQueries randomQueries = objectMapper.readValue(inputStream, RandomQueries.class);
            System.out.println(randomQueries.toString());

        }
    }

    @Test
    public void generateFixedQueryTest() {
        for (int i = 0; i < 3; i++) {
            System.out.println(fixedQueryGenerator.generateQuery(new Query()).entrySet());
        }
    }
}
