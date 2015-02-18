package org.opencb.opencga.catalog;

import com.mongodb.MongoCredential;
import org.junit.Before;
import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.beans.User;
import org.opencb.opencga.catalog.db.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.db.CatalogMongoDBAdaptor;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.TimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

/**
 * Created by jacobo on 28/01/15.
 */
public class CatalogFileManagerTest {

    CatalogFileManager catalogFileManager;
    private int studyId;
    private String userSessionId;
    private String adminSessionId;
    private CatalogManager catalogManager;

    @Before
    public void before() throws CatalogException, IOException {

        InputStream is = this.getClass().getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);


        MongoCredential mongoCredential = MongoCredential.createMongoCRCredential(
                properties.getProperty(CatalogManager.CATALOG_DB_USER, ""),
                properties.getProperty(CatalogManager.CATALOG_DB_DATABASE, ""),
                properties.getProperty(CatalogManager.CATALOG_DB_PASSWORD, "").toCharArray());

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                properties.getProperty(CatalogManager.CATALOG_DB_HOST, ""),
                Integer.parseInt(properties.getProperty(CatalogManager.CATALOG_DB_PORT, "0")));

        CatalogMongoDBAdaptor catalogDBAdaptor = new CatalogMongoDBAdaptor(dataStoreServerAddress, mongoCredential);


        catalogManager = new CatalogManager(catalogDBAdaptor, properties);

        //Create ADMIN
        catalogManager.createUser("admin", "name", "mi@mail.com", "asdf", "", null);
        catalogDBAdaptor.modifyUser("admin", new ObjectMap("role", User.Role.ADMIN));

        //Create USER
        catalogManager.createUser("user", "name", "mi@mail.com", "asdf", "", null);
        userSessionId = catalogManager.login("user", "asdf", "--").getResult().get(0).getString("sessionId");
        adminSessionId = catalogManager.login("admin", "asdf", "--").getResult().get(0).getString("sessionId");
        int projectId = catalogManager.createProject("user", "proj", "proj", "", "", null, userSessionId).getResult().get(0).getId();
        studyId = catalogManager.createStudy(projectId, "std", "std", Study.Type.CONTROL_SET, "", userSessionId).getResult().get(0).getId();

        catalogFileManager = new CatalogFileManager(catalogManager);
    }


    @Test
    public void updateTest() throws IOException, CatalogException {
        QueryResult<File> fileQueryResult;
        URI sourceUri;

        sourceUri = CatalogManagerTest.createDebugFile().toURI();
        fileQueryResult = catalogManager.createFile(
                studyId, File.Format.PLAIN, File.Bioformat.NONE, "item." + TimeUtils.getTimeMillis() + ".txt", "file at root", true, -1, userSessionId);
        catalogFileManager.upload(sourceUri, fileQueryResult.getResult().get(0), null, adminSessionId, false, false, true, false, 1000);

        sourceUri = CatalogManagerTest.createDebugFile().toURI();
        fileQueryResult = catalogManager.createFile(
                studyId, File.Format.PLAIN, File.Bioformat.NONE, "item." + TimeUtils.getTimeMillis() + ".txt", "file at root", true, -1, userSessionId);
        catalogFileManager.upload(sourceUri, fileQueryResult.getResult().get(0), null, adminSessionId, false, false, true, false, 100000000);

        sourceUri = CatalogManagerTest.createDebugFile().toURI();
        fileQueryResult = catalogManager.createFile(
                studyId, File.Format.PLAIN, File.Bioformat.NONE, "item." + TimeUtils.getTimeMillis() + ".txt", "file at root", true, -1, userSessionId);
        catalogFileManager.upload(sourceUri, fileQueryResult.getResult().get(0), null, adminSessionId, false, false, true, true);
    }

}
