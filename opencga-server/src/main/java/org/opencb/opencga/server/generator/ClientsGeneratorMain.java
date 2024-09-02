package org.opencb.opencga.server.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.logging.log4j.core.config.Configurator;
import org.opencb.opencga.server.generator.config.CommandLineConfiguration;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.generator.models.RestParameter;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

public class ClientsGeneratorMain {

    private static Logger logger;

    public static void main(String[] args) throws URISyntaxException {
        logger = LoggerFactory.getLogger(ClientsGeneratorMain.class);
        System.setProperty("opencga.log.file.enabled", "false");
        System.setProperty("opencga.log.level", "info");
        Configurator.reconfigure();

        List<Class<?>> classes = new ArrayList<>();
        classes.add(OrganizationWSServer.class);
        classes.add(UserWSServer.class);
        classes.add(ProjectWSServer.class);
        classes.add(StudyWSServer.class);
        classes.add(FileWSServer.class);
        classes.add(JobWSServer.class);
        classes.add(WorkflowWSServer.class);
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
            // Create CommandLineConfiguration and ClientsGenerator
            CommandLineConfiguration config = getCommandLineConfiguration();
            config.initialize();
            ClientsGenerator clientsGenerator = new ClientsGenerator(config);

            // Generate code for all clients
            String clientsGeneratorDir = "opencga-app/app/misc/clients/";
            String clientOutputDir = "opencga-client";
            String packagePath = "org.opencb.opencga.client.rest.clients";
            RestApi restApi = prepare(new RestApiParser().parse(classes, true));
            clientsGenerator.libraries(clientsGeneratorDir, restApi, packagePath, clientOutputDir);

            // Generate CLI code
            RestApi flatRestApi = prepare(new RestApiParser().parse(classes, true));
            clientsGenerator.cli(flatRestApi);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    private static CommandLineConfiguration getCommandLineConfiguration() throws IOException {
        // Loading the YAML file from the /resources folder
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource("cli-config.yaml")).getFile());

        // Mapping the config from the YAML file to the Configuration class
        logger.info("Loading CLI configuration from: " + file.getAbsolutePath());
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        return om.readValue(file, CommandLineConfiguration.class);
    }

    private static RestApi prepare(RestApi api) {
        // To process endpoints the parameter for each one must have a different name
        // Sometimes body parameter has the same name of a query parameter
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
                        // To avoid confusion, add "body_" to params has name conflict
                        for (RestParameter bodyParam : body.getData()) {
                            if (!bodyParam.isInnerParam() && aux.contains(bodyParam.getName())) {
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
