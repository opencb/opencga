package org.opencb.opencga.storage.benchmark.queryGenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQueries;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created by wasim on 31/10/18.
 */
public class FixedQueryGeneratorTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {


    @Test
    public void fixedQueryMappingTest() throws IOException {
        try (FileInputStream inputStream = new FileInputStream(Paths.get("src/test/resources/hsapiens/fixedQueries.yml").toFile());) {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            FixedQueries fixedQueries = objectMapper.readValue(inputStream, FixedQueries.class);

            assertEquals(fixedQueries.getBaseQuery().keySet().size(), 1);
            assertEquals(fixedQueries.getBaseQuery().get("summary"), "true");
            assertEquals(fixedQueries.getQueries().size(), 7);
            assertEquals(fixedQueries.getSessionIds().size(), 2);

            assertEquals(fixedQueries.getQueries().get(0).getId(), "RegionAndBiotype");
            assertEquals(fixedQueries.getQueries().get(0).getDescription(), "Purpose of this query");
            assertEquals(fixedQueries.getQueries().get(0).getQuery().keySet().size(), 4);
            assertEquals(fixedQueries.getQueries().get(0).getQuery().keySet().size(), 4);
            assertEquals(fixedQueries.getQueries().get(0).getQuery().get("region"), "22:16052853-16054112");
            assertEquals(fixedQueries.getQueries().get(0).getQuery().get("gene"), "BRCA2");
            assertEquals(fixedQueries.getQueries().get(0).getQuery().get("biotype"), "coding");
            assertEquals(fixedQueries.getQueries().get(0).getQuery().get("populationFrequencyMaf"), "1kG_phase3:ALL>0.1");
            assertEquals(fixedQueries.getQueries().get(0).getTolerationThreshold(), 300);

        }
    }
}
