package org.opencb.opencga.app.migrations.v3_0_0;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Catalog;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.DatabaseCredentials;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;

import java.util.Collections;

public class OrganizationMigrationTest {

    @Test
    public void testTestRun() throws Exception {
        String adminPassword = "4dMiNiStRaT0r.";
        Configuration configuration = new Configuration()
                .setWorkspace("/tmp/opencga")
                .setJobDir("/tmp/opencga/jobs")
                .setCatalog(new Catalog(
                        new DatabaseCredentials(Collections.singletonList("localhost"), null, null),
                        null
                ))
                .setDatabasePrefix("opencga");

        OrganizationMigration organizationMigration = new OrganizationMigration(configuration, adminPassword, "demo");
        organizationMigration.run();

        // Test data access
        String password = "MyStR0nGP4sSwOrD.";
        CatalogManager catalogManager1 = new CatalogManager(configuration);
        AuthenticationResponse authenticationResponse = catalogManager1.getUserManager().loginAsAdmin(adminPassword);
        AuthenticationResponse demo = catalogManager1.getUserManager().login("demo", "demo", password);
        catalogManager1.getFileManager().search(null, new Query(), QueryOptions.empty(), demo.getToken());
        catalogManager1.getFileManager().create(null, new FileCreateParams().setPath("data.txt").setContent("This is the content").setType(File.Type.FILE), false, demo.getToken());

        catalogManager1.getOrganizationManager().create(new OrganizationCreateParams().setId("myOrg"), null, authenticationResponse.getToken());
        catalogManager1.getUserManager().create("myOrg", new User().setId("user").setName("user"), password, authenticationResponse.getToken());
        catalogManager1.getOrganizationManager().update("myOrg", new OrganizationUpdateParams().setOwner("user"), null, authenticationResponse.getToken());
        String myOrgToken = catalogManager1.getUserManager().login("myOrg", "user", password).getToken();
        catalogManager1.getProjectManager().create(new ProjectCreateParams().setId("id").setOrganism(new ProjectOrganism("Homo sapiens", "GRCh38")), null, myOrgToken);
        catalogManager1.getStudyManager().create("id", new Study().setId("myStudy"), null, myOrgToken);
        catalogManager1.getFileManager().create(null, new FileCreateParams().setPath("data2.txt").setContent("This is the content 2").setType(File.Type.FILE), false, myOrgToken);

        System.out.println(authenticationResponse);
        System.out.println(demo);

//        use opencga_opencga_catalog
//        db.dropDatabase()
//        use opencga_demo_catalog
//        db.dropDatabase()
//        use opencga_myorg_catalog
//        db.dropDatabase()
    }
}