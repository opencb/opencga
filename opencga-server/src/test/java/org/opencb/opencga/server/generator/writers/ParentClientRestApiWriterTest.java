package org.opencb.opencga.server.generator.writers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.server.generator.RestApiParser;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.config.CommandLineConfiguration;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.generator.writers.cli.OptionsCliRestApiWriter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(ShortTests.class)
public class ParentClientRestApiWriterTest {

    @Test
    public void getCommandName() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        CommandLineConfiguration config;
        try (InputStream inputStream = getClass().getResource("/cli-config.yaml").openStream()) {
             config = objectMapper.readValue(inputStream, CommandLineConfiguration.class);
        }

        ApiCommonsImpl apiCommons = new ApiCommonsImpl();
        List<Class<?>> classes = apiCommons.getApiClasses();
        RestApi restApi = new RestApiParser().parse(classes, true);

        OptionsCliRestApiWriter optionsCliRestApiWriter = new OptionsCliRestApiWriter(restApi, config);
        String commandName = optionsCliRestApiWriter.getCommandName(
                new RestCategory().setPath("/{apiVersion}/operation"),
                new RestEndpoint().setPath("/{apiVersion}/operation/variant/secondaryIndex"));
        assertEquals("secondaryIndex-variant", commandName);
        assertEquals("variant-secondary-index", ParentClientRestApiWriter.reverseCommandName(commandName));
    }
}