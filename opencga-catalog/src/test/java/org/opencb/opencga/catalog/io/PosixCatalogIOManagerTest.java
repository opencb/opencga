package org.opencb.opencga.catalog.io;

import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.opencga.lib.common.IOUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class PosixCatalogIOManagerTest {

    static PosixCatalogIOManager posixCatalogIOManager;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("Testing PosixIOManagerTest");
        Path path = Paths.get("/tmp").resolve("opencga");
        try {
            if (Files.exists(path)) {
                IOUtils.deleteDirectory(path);
            }
            Files.createDirectory(path);
            posixCatalogIOManager = new PosixCatalogIOManager("/tmp/opencga", true);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateAccount() throws Exception {
        String userId = "imedina";
        Path userPath = posixCatalogIOManager.createUser(userId);
        assertTrue(Files.exists(userPath));
        assertEquals("/tmp/opencga/users/"+userId, userPath.toString());

        posixCatalogIOManager.deleteUser(userId);
        assertFalse(Files.exists(userPath));
    }

    @Test
    public void testCreateStudy() throws Exception {
        String userId = "imedina";
        String projectId = "1000g";

        Path userPath = posixCatalogIOManager.createUser(userId);

        Path projectPath = posixCatalogIOManager.createProject(userId, projectId);
        assertTrue(Files.exists(projectPath));
        assertEquals(userPath.toString()+"/projects/"+projectId, projectPath.toString());

        Path studyPath = posixCatalogIOManager.createStudy(userId, projectId, "phase1");
        assertTrue(Files.exists(studyPath));
        assertTrue(Files.exists(studyPath.resolve("data")));
        assertTrue(Files.exists(studyPath.resolve("analysis")));
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