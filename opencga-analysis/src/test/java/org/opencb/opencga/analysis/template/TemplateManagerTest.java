package org.opencb.opencga.analysis.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.template.config.TemplateManifest;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;

import java.net.URL;
import java.nio.file.Paths;

public class TemplateManagerTest {

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    @Before
    public void before() throws Exception {
        catalogManagerResource.before();
    }

    @After
    public void after() {
        catalogManagerResource.after();
    }

    @Test
    public void test() throws Exception {
        CatalogManager catalogManager = catalogManagerResource.getCatalogManager();
        String adminToken = catalogManager.getUserManager().loginAsAdmin("admin").getToken();

        catalogManager.getUserManager().create(new User().setId("user1").setName("User 1").setAccount(new Account().setType(Account.AccountType.FULL)),
                "password", adminToken);
        catalogManager.getUserManager().create(new User().setId("user2").setName("User 2").setAccount(new Account().setType(Account.AccountType.GUEST)), "password", adminToken);
        catalogManager.getUserManager().create(new User().setId("user3").setName("User 3").setAccount(new Account().setType(Account.AccountType.GUEST)), "password", adminToken);
        catalogManager.getUserManager().create(new User().setId("user4").setName("User 4").setAccount(new Account().setType(Account.AccountType.GUEST)), "password", adminToken);

        String token = catalogManager.getUserManager().login("user1", "password").getToken();
        catalogManager.getProjectManager().create("project", "Project", "", "name", "common", "GRCh38", QueryOptions.empty(), token);

        URL resource = this.getClass().getResource("/template/manifest.yml");
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        TemplateManifest manifest = objectMapper.readValue(resource, TemplateManifest.class);

        TemplateManager templateManager = new TemplateManager(catalogManager, false, false, token);
        templateManager.execute(manifest, Paths.get(resource.toURI()).getParent());

    }

}