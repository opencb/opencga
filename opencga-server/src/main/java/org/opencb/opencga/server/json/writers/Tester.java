package org.opencb.opencga.server.json.writers;

import org.opencb.opencga.server.json.RestApiParser;
import org.opencb.opencga.server.json.beans.Category;
import org.opencb.opencga.server.json.beans.Endpoint;
import org.opencb.opencga.server.json.beans.Parameter;
import org.opencb.opencga.server.json.beans.RestApi;
import org.opencb.opencga.server.json.config.CommandLineConfiguration;
import org.opencb.opencga.server.json.config.ConfigurationManager;
import org.opencb.opencga.server.json.writers.cli.ExecutorsCliRestApiWriter;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Tester {

    private static RestApi restApi;
    private static CommandLineConfiguration config;
    private static ExecutorsCliRestApiWriter ex;

    public static void main(String[] args) {
       init();

        Category category= ex.getAvailableCategories().get("samples");
        for (Endpoint endpoint : category.getEndpoints()) {
            System.out.println(""+endpoint.getBodyParamsObject());
        }

    }
    public static void init(){
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
        restApi = RestApiParser.getApi(classes);

        try {
            config = ConfigurationManager.setUp(restApi);
            config.initialize();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ex=new ExecutorsCliRestApiWriter(restApi, config);
        Category category= ex.getAvailableCategories().get("samples");
        Set<String> imports = new HashSet<>();
        for (Endpoint endpoint : category.getEndpoints()) {
            imports.add(endpoint.getResponseClass());
            for (Parameter parameter : endpoint.getParameters()) {
                if (ex.isValidImport(parameter.getTypeClass())) {
                    imports.add(parameter.getTypeClass());
                }
            }
        }

    }

}
