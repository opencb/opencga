package org.opencb.opencga.server.json;

import org.apache.log4j.Logger;
import org.opencb.opencga.server.json.config.CommandLineConfiguration;
import org.opencb.opencga.server.json.config.ConfigurationManager;
import org.opencb.opencga.server.json.models.RestApi;
import org.opencb.opencga.server.json.models.RestCategory;
import org.opencb.opencga.server.json.models.RestEndpoint;
import org.opencb.opencga.server.json.models.RestParameter;
import org.opencb.opencga.server.json.writers.cli.AutoCompleteWriter;
import org.opencb.opencga.server.json.writers.cli.ExecutorsCliRestApiWriter;
import org.opencb.opencga.server.json.writers.cli.OptionsCliRestApiWriter;
import org.opencb.opencga.server.json.writers.cli.ParserCliRestApiWriter;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ClientsGenerator {

    private static final Logger logger = Logger.getLogger(ClientsGenerator.class);
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
            config = ConfigurationManager.setUp(restApi);
            config.initialize();

            cli();
        } catch (Exception e) {
            e.printStackTrace();
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
