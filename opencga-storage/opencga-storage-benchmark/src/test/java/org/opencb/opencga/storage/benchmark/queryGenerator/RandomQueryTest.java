package org.opencb.opencga.storage.benchmark.queryGenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;
import org.opencb.opencga.storage.benchmark.variant.queries.RandomQueries;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created by wasim on 01/11/18.
 */
public class RandomQueryTest {

    @Test
    public void randomQueryMappingTest() throws IOException {
        try (FileInputStream inputStream = new FileInputStream(Paths.get("src/test/resources/hsapiens/randomQueries.yml").toFile());) {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            RandomQueries randomQueries = objectMapper.readValue(inputStream, RandomQueries.class);

            assertEquals(randomQueries.getRegions().get(0).getChromosome() , "1");
            assertEquals(randomQueries.getRegions().get(0).getStart(), 1);
            assertEquals(randomQueries.getRegions().get(0).getEnd(), 249250621);
            assertEquals(randomQueries.getGene().get(0),"DKFZP434A062");
            assertEquals(randomQueries.getType().size(), 2);
            assertEquals(randomQueries.getQual().getOperators().size(), 1);
            assertEquals(randomQueries.getProteinSubstitution().get(0).getOperators().size(),2);
        }
    }
}
