package org.opencb.opencga.server.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.server.generator.config.CommandLineConfiguration;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.models.RestParameter;
import org.opencb.opencga.server.generator.writers.cli.AutoCompleteWriter;
import org.opencb.opencga.server.generator.writers.cli.ExecutorsCliRestApiWriter;
import org.opencb.opencga.server.generator.writers.cli.OptionsCliRestApiWriter;
import org.opencb.opencga.server.generator.writers.cli.ParserCliRestApiWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.List;

public class ClientsGenerator {

    private final CommandLineConfiguration config;
    private static Logger logger;

    public ClientsGenerator(CommandLineConfiguration config) {
        this.config = config;
        logger = LoggerFactory.getLogger(ClientsGenerator.class);
    }

    public void libraries(RestApi restApi) throws IOException {
        File outDir = new File("restApi.json");
        String restApiFilePath = outDir.getAbsolutePath();
        logger.info("Writing RestApi object temporarily in {}", restApiFilePath);

        ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
        mapper.writeValue(outDir, restApi);

        generateLibrary("java", restApiFilePath, "opencga-client/src/main/java/org/opencb/opencga/client/rest/clients/");
        generateLibrary("python", restApiFilePath, "opencga-client/src/main/python/pyopencga/rest_clients/");
        generateLibrary("javascript", restApiFilePath, "opencga-client/src/main/javascript/");
        generateLibrary("r", restApiFilePath, "opencga-client/src/main/R/R/");

        logger.info("Deleting temporal RestApi object from {}", restApiFilePath);
        Files.delete(outDir.toPath());
    }

    public void generateLibrary(String language, String restFilePath, String outDir) {
        String binary = "opencga-app/app/misc/clients/" + language + "_client_generator.py";

        ProcessBuilder processBuilder = new ProcessBuilder("python3", binary, restFilePath, outDir);
        Process p;
        try {
            p = processBuilder.start();
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                logger.debug("{} library generator: {}", language, line);
            }
            p.waitFor();
            input.close();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error executing cli: " + e.getMessage(), e);
        }

        if (p.exitValue() != 0) {
            throw new RuntimeException("Error with " + language + " library generator");
        }
    }

    public void cli(RestApi flatRestApi) {
        logger.info("Creating CLI options files in folder " + new File(config.getOptions().getOptionsOutputDir()).getAbsolutePath());
        OptionsCliRestApiWriter optionsCliRestApiWriter = new OptionsCliRestApiWriter(flatRestApi, config);
        optionsCliRestApiWriter.write();

        logger.info("Creating CLI executors files in folder " + new File(config.getOptions().getExecutorsOutputDir()).getAbsolutePath());
        ExecutorsCliRestApiWriter executorsCliRestApiWriter = new ExecutorsCliRestApiWriter(flatRestApi, config);
        executorsCliRestApiWriter.write();

        logger.info("Creating CLI parser file in folder " + new File(config.getOptions().getOutputDir()).getAbsolutePath());
        ParserCliRestApiWriter parserCliRestApiWriter = new ParserCliRestApiWriter(flatRestApi, config);
        parserCliRestApiWriter.write();

        logger.info("Creating CLI parser file in folder " + new File(config.getOptions().getOutputDir()).getAbsolutePath());
        AutoCompleteWriter autoCompleteWriter = new AutoCompleteWriter(flatRestApi, config);
        autoCompleteWriter.write();
    }

    private static void addParameter(RestParameter parameter, List<RestParameter> parameters) {
        if (!parameter.isComplex()) {
            parameters.add(parameter);
        } else if (isGenericCollection(parameter)) {
            parameters.add(parameter);
        } else if (parameter.isEnum()) {
            parameters.add(parameter);
        }
    }

    private static boolean isGenericCollection(RestParameter parameter) {
        String[] collections = {"java.util.Map<java.lang.String,java.lang.String>", "java.util.List<java.lang.String>",
                "java.util.Map<java.lang.String,java.lang.Integer>", "java.util.Map<java.lang.String,java.lang.Long>",
                "java.util.Map<java.lang.String,java.lang.Float>"};
        return ArrayUtils.contains(collections, parameter.getGenericType());
    }
}
