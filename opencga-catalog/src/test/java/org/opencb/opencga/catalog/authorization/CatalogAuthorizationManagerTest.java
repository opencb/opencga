package org.opencb.opencga.catalog.authorization;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Acl;
import org.opencb.opencga.catalog.models.Study;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created on 19/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationManagerTest {

    private final String user1 = "user1";
    private final String user2 = "user2";
    private final String user3 = "user3";
    private final String password = "1234";
    private CatalogManager catalogManager;
    private String user1SessionId;
    private String user2SessionId;
    private String user3SessionId;
    private int p1;
    private int s1;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private int data;
    private int subFolder;

    @Before
    public void before () throws Exception {
        InputStream is = CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        CatalogManagerTest.clearCatalog(properties);

        catalogManager = new CatalogManager(properties);

        catalogManager.createUser(user1, user1, "email@ccc.ccc", password, "ASDF", null);
        catalogManager.createUser(user2, user2, "email@ccc.ccc", password, "ASDF", null);
        catalogManager.createUser(user3, user3, "email@ccc.ccc", password, "ASDF", null);

        user1SessionId = catalogManager.login(user1, password, "localhost").first().get("sessionId").toString();
        user2SessionId = catalogManager.login(user2, password, "localhost").first().get("sessionId").toString();
        user3SessionId = catalogManager.login(user3, password, "localhost").first().get("sessionId").toString();

        p1 = catalogManager.createProject(user1, "p1", "p1", null, null, null, user1SessionId).first().getId();
        s1 = catalogManager.createStudy(p1, "s1", "s1", Study.Type.CASE_CONTROL, null, user1SessionId).first().getId();
        data = catalogManager.searchFile(s1, new QueryOptions(CatalogFileDBAdaptor.FileFilterOption.path.toString(), "data/"), user1SessionId).first().getId();
        subFolder = catalogManager.createFolder(s1, Paths.get("data/subfolder/"), false, null, user1SessionId).first().getId();

        catalogManager.shareProject(p1, new Acl(user2, true, true, true, true), user1SessionId);
        catalogManager.shareStudy(s1, new Acl(user2, true, true, true, true), user1SessionId);
        catalogManager.shareFile(subFolder, new Acl(user2, true, true, true, true), user1SessionId);

    }

    @After
    public void after() throws Exception {
        catalogManager.close();
    }

    @Test
    public void readProject() throws CatalogException {
        catalogManager.getProject(p1, null, user2SessionId);
    }

    @Test
    public void readProjectDenny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getProject(p1, null, user3SessionId);
    }

    @Test
    public void readStudy() throws CatalogException {
        catalogManager.getStudy(s1, user2SessionId);
    }

    @Test
    public void readStudyDenny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudy(s1, user3SessionId);
    }

    @Test
    public void readFile() throws CatalogException {
        catalogManager.getFile(subFolder, user2SessionId);
    }

    @Test
    public void readFileDenny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data, user3SessionId);
    }


}