package org.opencb.opencga.catalog.io;

import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.opencga.core.common.IOUtils;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.*;

public class PosixCatalogIOManagerTest {

    static CatalogIOManager posixCatalogIOManager;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("Testing PosixIOManagerTest");
        Path path = Paths.get("/tmp").resolve("opencga");
        try {
            if (Files.exists(path)) {
                IOUtils.deleteDirectory(path);
            }
            Files.createDirectory(path);
            Properties properties = new Properties();
            properties.setProperty("CATALOG.FILE.ROOTDIR", path.toUri().toString());
            posixCatalogIOManager = new PosixCatalogIOManager(properties);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateAccount() throws Exception {
        String userId = "imedina";
        URI userUri = posixCatalogIOManager.createUser(userId);

        Path userPath = Paths.get(userUri);
        assertTrue(Files.exists(userPath));
        assertEquals("/tmp/opencga/users/" + userId + "/", userUri.getPath());

        posixCatalogIOManager.deleteUser(userId);
        assertFalse(Files.exists(userPath));
    }

    @Test
    public void testCreateStudy() throws Exception {
        String userId = "imedina";
        String projectId = "1000g";

        Path userPath = Paths.get(posixCatalogIOManager.createUser(userId));

        Path projectPath = Paths.get(posixCatalogIOManager.createProject(userId, projectId));
        assertTrue(Files.exists(projectPath));
        assertEquals(userPath.toString()+"/projects/"+projectId , projectPath.toString());

        Path studyPath = Paths.get(posixCatalogIOManager.createStudy(userId, projectId, "phase1"));
        assertTrue(Files.exists(studyPath));
//        assertTrue(Files.exists(studyPath.resolve("data")));
//        assertTrue(Files.exists(studyPath.resolve("analysis")));
        assertEquals(projectPath.toString()+"/phase1", studyPath.toString());

//        posixIOManager.deleteStudy(userId, projectId, "phase1");
//        assertFalse(Files.exists(studyPath));
//
//        posixIOManager.deleteProject(userId, projectId);
//        assertFalse(Files.exists(projectPath));
//
//        posixIOManager.deleteUser(userId);
//        assertFalse(Files.exists(studyPath));
    }

}