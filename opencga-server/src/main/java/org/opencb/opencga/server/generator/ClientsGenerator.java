package org.opencb.opencga.server.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.server.generator.config.CommandLineConfiguration;
import org.opencb.opencga.server.generator.config.ConfigurationManager;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.generator.models.RestParameter;
import org.opencb.opencga.server.generator.writers.cli.AutoCompleteWriter;
import org.opencb.opencga.server.generator.writers.cli.ExecutorsCliRestApiWriter;
import org.opencb.opencga.server.generator.writers.cli.OptionsCliRestApiWriter;
import org.opencb.opencga.server.generator.writers.cli.ParserCliRestApiWriter;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class ClientsGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ClientsGenerator.class);
    private static RestApi restApi;
    private static CommandLineConfiguration config;

    public static void main(String[] args) {
        List<Class> classes = new ArrayList<>();
        classes.add(UserWSServer.class);
        classes.add(ProjectWSServer.class);
        classes.add(StudyWSServer.class);
        classes.add(FileWSServer.class);
        classes.add(JobWSServer.class);
        classes.add(SampleWSServer.class);
        classes.add(IndividualWSServer.class);
        classes.add(FamilyWSServer.class);
        classes.add(CohortWSServer.class);
        classes.add(PanelWSServer.class);
        classes.add(AlignmentWebService.class);
        classes.add(VariantWebService.class);
        classes.add(ClinicalWebService.class);
        classes.add(VariantOperationWebService.class);
        classes.add(MetaWSServer.class);
        classes.add(Ga4ghWSServer.class);
        classes.add(AdminWSServer.class);

        try {
            restApi = prepare(new RestApiParser().parse(classes));
            config = ConfigurationManager.setUp();
            config.initialize();

            libraries();
            cli();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void libraries() throws IOException {
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

    private static void generateLibrary(String language, String restFilePath, String outDir) {
        String binary = "opencga-app/app/misc/clients/" + language + "_client_generator.py";

        ProcessBuilder processBuilder = new ProcessBuilder("python3", binary, restFilePath, outDir);
        processBuilder.redirectErrorStream(true);
        Process p;
        try {
            p = processBuilder.start();
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                logger.info("{} library generator: {}", language, line);
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

    private static void cli() {
        logger.info("Creating CLI options files in folder " + new File(config.getOptions().getOptionsOutputDir()).getAbsolutePath());
        OptionsCliRestApiWriter optionsCliRestApiWriter = new OptionsCliRestApiWriter(restApi, config);
        optionsCliRestApiWriter.write();

        logger.info("Creating CLI executors files in folder " + new File(config.getOptions().getExecutorsOutputDir()).getAbsolutePath());
        ExecutorsCliRestApiWriter executorsCliRestApiWriter = new ExecutorsCliRestApiWriter(restApi, config);
        executorsCliRestApiWriter.write();

        logger.info("Creating CLI parser file in folder " + new File(config.getOptions().getOutputDir()).getAbsolutePath());
        ParserCliRestApiWriter parserCliRestApiWriter = new ParserCliRestApiWriter(restApi, config);
        parserCliRestApiWriter.write();

        logger.info("Creating CLI parser file in folder " + new File(config.getOptions().getOutputDir()).getAbsolutePath());
        AutoCompleteWriter autoCompleteWriter = new AutoCompleteWriter(restApi, config);
        autoCompleteWriter.write();
    }

    private static RestApi prepare(RestApi api) {
        //To process endpoints the parameter for each one must have a different name
        //Sometimes body parameter has the same name of a query parameter
        for (RestCategory restCategory : api.getCategories()) {
            for (RestEndpoint restEndpoint : restCategory.getEndpoints()) {
                List<String> aux = new ArrayList<>();
                for (RestParameter restParameter : restEndpoint.getParameters()) {
                    aux.add(restParameter.getName());
                }
                for (RestParameter restParameter : restEndpoint.getParameters()) {
                    if (restParameter.getData() != null) {
                        for (RestParameter body_Rest_parameter : restParameter.getData()) {
                            if (aux.contains(body_Rest_parameter.getName())) {
                                body_Rest_parameter.setName("body_" + body_Rest_parameter.getName());
                            }
                        }
                    }
                }
            }
        }

        return api;
    }
}
