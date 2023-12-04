package org.opencb.opencga.templates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.templates.TemplateManager;
import org.opencb.opencga.catalog.templates.config.TemplateManifest;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.net.URI;
import java.nio.file.Paths;

@Category(MediumTests.class)
public class TemplateManagerTest extends AbstractManagerTest {

    @Test
    public void test() throws Exception {
        CatalogManager catalogManager = catalogManagerResource.getCatalogManager();

        catalogManager.getUserManager().create(organizationId, new User().setId("user1").setName("User 1").setAccount(new Account().setType(Account.AccountType.FULL)),
                TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(organizationId, new User().setId("user2").setName("User 2").setAccount(new Account().setType(Account.AccountType.GUEST)), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(organizationId, new User().setId("user3").setName("User 3").setAccount(new Account().setType(Account.AccountType.GUEST)), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(organizationId, new User().setId("user4").setName("User 4").setAccount(new Account().setType(Account.AccountType.GUEST)), TestParamConstants.PASSWORD, opencgaToken);

        catalogManager.getProjectManager().create("project", "Project", "", "hsapiens", "common", "GRCh38", QueryOptions.empty(), ownerToken);
        catalogManager.getStudyManager().create("project", new Study().setId("study"), QueryOptions.empty(), ownerToken);

        URI resource = catalogManagerResource.getResourceUri("templates/manifest.yml");
        catalogManagerResource.getResourceUri("templates/families.members.txt");
        catalogManagerResource.getResourceUri("templates/families.txt");
        catalogManagerResource.getResourceUri("templates/individuals.samples.txt");
        catalogManagerResource.getResourceUri("templates/individuals.txt");
        catalogManagerResource.getResourceUri("templates/samples.txt");

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        TemplateManifest manifest = objectMapper.readValue(resource.toURL(), TemplateManifest.class);
        TemplateManager templateManager = new TemplateManager(catalogManager, false, false, ownerToken);
        templateManager.execute(manifest, Paths.get(resource).getParent());

        templateManager = new TemplateManager(catalogManager, true, true, ownerToken);
        templateManager.execute(manifest, Paths.get(resource).getParent());
    }

    @Test
    public void test_yaml() throws Exception {
        CatalogManager catalogManager = catalogManagerResource.getCatalogManager();

        catalogManager.getUserManager().create(organizationId, new User().setId("user1").setName("User 1").setAccount(new Account().setType(Account.AccountType.FULL)),
                TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(organizationId, new User().setId("user2").setName("User 2").setAccount(new Account().setType(Account.AccountType.GUEST)), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(organizationId, new User().setId("user3").setName("User 3").setAccount(new Account().setType(Account.AccountType.GUEST)), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(organizationId, new User().setId("user4").setName("User 4").setAccount(new Account().setType(Account.AccountType.GUEST)), TestParamConstants.PASSWORD, opencgaToken);

        catalogManager.getProjectManager().create("project", "Project", "", "hsapiens", "common", "GRCh38", QueryOptions.empty(), ownerToken);
        catalogManager.getStudyManager().create("project", new Study().setId("study"), QueryOptions.empty(), ownerToken);

        URI resource = catalogManagerResource.getResourceUri("templates_yaml/manifest.yml");
        catalogManagerResource.getResourceUri("templates_yaml/families.members.txt");
        catalogManagerResource.getResourceUri("templates_yaml/families.txt");
        catalogManagerResource.getResourceUri("templates_yaml/samples.txt");
        catalogManagerResource.getResourceUri("templates_yaml/individuals.yml");

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        TemplateManifest manifest = objectMapper.readValue(resource.toURL(), TemplateManifest.class);
        TemplateManager templateManager = new TemplateManager(catalogManager, false, false, ownerToken);
        templateManager.execute(manifest, Paths.get(resource).getParent());

        templateManager = new TemplateManager(catalogManager, true, true, ownerToken);
        templateManager.execute(manifest, Paths.get(resource).getParent());
    }

}