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

    public void libraries(String clientsGeneratorDir, RestApi restApi, String packagePath, String clientOutputDir) throws IOException {
        File outDir = new File("restApi.json");
        String restApiFilePath = outDir.getAbsolutePath();
        logger.info("Writing RestApi object temporarily in {}", restApiFilePath);

        ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
        mapper.writeValue(outDir, restApi);

        generateLibrary(clientsGeneratorDir, "java", restApiFilePath, clientOutputDir + "/src/main/java/" + packagePath.replaceAll("\\.", "/"));
        generateLibrary(clientsGeneratorDir, "python", restApiFilePath, clientOutputDir + "/src/main/python/pyopencga/rest_clients/");
        generateLibrary(clientsGeneratorDir, "javascript", restApiFilePath, clientOutputDir + "/src/main/javascript/");
        generateLibrary(clientsGeneratorDir, "r", restApiFilePath, clientOutputDir + "/src/main/R/R/");

        //logger.info("Deleting temporal RestApi object from {}", restApiFilePath);
        Files.delete(outDir.toPath());
    }

    private void generateLibrary(String clientsGeneratorDir, String language, String restFilePath, String outDir) {
        logger.info("clientsGeneratorDir " + clientsGeneratorDir);
        logger.info("language " + language);
        logger.info("restFilePath " + restFilePath);
        logger.info("outDir " + outDir);
        String binary = clientsGeneratorDir + "/" + language + "_client_generator.py";
        ProcessBuilder processBuilder = new ProcessBuilder("python3", binary, restFilePath, outDir);
        Process p;
        try {
            p = processBuilder.start();
            BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            p.waitFor();
            if (p.exitValue() != 0) {
                String lineError;
                while ((lineError = error.readLine()) != null) {
                    logger.error("{} library generator: {}", language, lineError);
                    System.err.println(language + " library generator: " + lineError);
                }
                throw new RuntimeException("Error with " + language + " library generator");
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error executing cli: " + e.getMessage(), e);
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
