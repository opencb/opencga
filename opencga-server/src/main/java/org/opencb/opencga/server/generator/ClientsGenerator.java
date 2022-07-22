package org.opencb.opencga.server.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.core.config.Configurator;
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
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClientsGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ClientsGenerator.class);
    // private static RestApi restApi;
    private static CommandLineConfiguration config;

    public static void main(String[] args) throws URISyntaxException {
        System.setProperty("opencga.log.file.enabled", "false");
        System.setProperty("opencga.log.level", "info");
        Configurator.reconfigure();

        List<Class<?>> classes = new ArrayList<>();
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
            RestApi restApi = prepare(new RestApiParser().parse(classes, true));
            config = ConfigurationManager.setUp();
            config.initialize();

            libraries(restApi);


            RestApi flatRestApi = prepare(new RestApiParser().parse(classes, true));
            cli(flatRestApi);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void libraries(RestApi restApi) throws IOException {
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

    private static void cli(RestApi flatRestApi) {

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

    private static RestApi prepare(RestApi api) {
        //To process endpoints the parameter for each one must have a different name
        //Sometimes body parameter has the same name of a query parameter
        for (RestCategory restCategory : api.getCategories()) {
            for (RestEndpoint restEndpoint : restCategory.getEndpoints()) {
                Set<String> aux = new HashSet<>();
                for (RestParameter restParameter : restEndpoint.getParameters()) {
                    if (!restParameter.isInnerParam()) {
                        aux.add(restParameter.getName());
                    }
                }
                RestParameter body = restEndpoint.getParameterBody();
                if (body != null && body.getData() != null) {
                    boolean overlappingParamNames = false;
//                    for (RestParameter bodyParam : body.getData()) {
//                        if (!bodyParam.isInnerParam() && aux.contains(bodyParam.getName())) {
//                            bodyParam.setName("body_" + bodyParam.getName());
//                            anyMatch = true;
//                        }
//                    }
                    for (RestParameter bodyParam : body.getData()) {
                        if (!bodyParam.isInnerParam() && aux.contains(bodyParam.getName())) {
                            overlappingParamNames = true;
                        }
                    }
                    if (overlappingParamNames) {
                        logger.info("Overlapping param names at " + restEndpoint.getPath());
                        // To avoid confusion, add "body_" to all params
                        for (RestParameter bodyParam : body.getData()) {
                            if (!bodyParam.isInnerParam()) {
                                bodyParam.setName("body_" + bodyParam.getName());
                            }
                        }
                    }
                }
            }
        }

        return api;
    }
}
