package org.opencb.opencga.server.generator.models;

import org.junit.Test;
import org.opencb.opencga.server.generator.RestApiParser;
import org.opencb.opencga.server.generator.config.CategoryConfig;
import org.opencb.opencga.server.generator.config.CommandLineConfiguration;
import org.opencb.opencga.server.generator.config.ConfigurationManager;
import org.opencb.opencga.server.generator.writers.ParentClientRestApiWriter;
import org.opencb.opencga.server.rest.CohortWSServer;

public class RestParameterManagerTest {


    @Test
    public void testRestParameterWrapper() throws Exception {
        RestApi r = new RestApiParser().parse(CohortWSServer.class);
        CommandLineConfiguration config = ConfigurationManager.setUp();
        CategoryConfig catConfig = null;
        for (CategoryConfig c : config.getApiConfig().getCategoryConfigList()) {
            //  System.out.println("CATEGORY CONFIG ->" + c.getName());
            if (c.getName().equals("cohorts")) {
                catConfig = c;
            }
        }
        for (RestCategory restCategory : r.getCategories()) {
            for (RestEndpoint restEndpoint : restCategory.getEndpoints()) {
                String commandName = ParentClientRestApiWriter.getCommandName(restCategory, restEndpoint);
                RestParameterManager rp = new RestParameterManager(restEndpoint, config, catConfig, commandName);
                System.out.println(commandName);
                System.out.println(rp.getJCommanderOptions());
            }
        }
    }


}
