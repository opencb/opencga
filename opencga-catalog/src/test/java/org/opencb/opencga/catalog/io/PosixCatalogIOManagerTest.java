/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.io;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.file.FileContent;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class PosixCatalogIOManagerTest {

    static CatalogIOManager posixCatalogIOManager;

    @Before
    public void before() {
        System.out.println("Testing PosixIOManagerTest");
        Path path = Paths.get("/tmp").resolve("opencga");
        try {
            if (Files.exists(path)) {
                IOUtils.deleteDirectory(path);
            }
            Files.createDirectory(path);

            Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml").openStream());
            posixCatalogIOManager = new PosixCatalogIOManager(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateAccount() throws Exception {
        String userId = "imedina";
        URI userUri = posixCatalogIOManager.createUser(userId);

        Path userPath = Paths.get(userUri);
        assertTrue(Files.exists(userPath));
        assertEquals("/tmp/opencga/sessions/users/" + userId, userUri.getPath());

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
        assertEquals(userPath.toString() + "/projects/" + projectId, projectPath.toString());

        Path studyPath = Paths.get(posixCatalogIOManager.createStudy(userId, projectId, "phase1"));
        assertTrue(Files.exists(studyPath));
//        assertTrue(Files.exists(studyPath.resolve("data")));
//        assertTrue(Files.exists(studyPath.resolve("analysis")));
        assertEquals(projectPath.toString() + "/phase1", studyPath.toString());

//        posixIOManager.deleteStudy(userId, projectId, "phase1");
//        assertFalse(Files.exists(studyPath));
//
//        posixIOManager.deleteProject(userId, projectId);
//        assertFalse(Files.exists(projectPath));
//
//        posixIOManager.deleteUser(userId);
//        assertFalse(Files.exists(studyPath));
    }

    @Test
    public void testHead() throws URISyntaxException, CatalogIOException {
        Path path =  Paths.get(this.getClass().getClassLoader().getResource("20130606_g1k.ped").toURI());

        FileContent fileContent = posixCatalogIOManager.head(path, 10);
        FileContent fileContent2 = posixCatalogIOManager.content(path, 0, 0);

        assertEquals(fileContent2.getContent(), fileContent.getContent());
        System.out.println(fileContent);
        System.out.println();
        System.out.println(posixCatalogIOManager.tail(path, 10).getContent());
    }

    @Test
    public void testGrep() throws URISyntaxException, CatalogIOException {
        Path path =  Paths.get(this.getClass().getClassLoader().getResource("20130606_g1k.ped").toURI());

        FileContent fileContent = posixCatalogIOManager.grep(path, "hG01880", 0, false);
        assertTrue(StringUtils.isEmpty(fileContent.getContent()));

        fileContent = posixCatalogIOManager.grep(path, "hG01880", 0, true);
        assertTrue(StringUtils.isNotEmpty(fileContent.getContent()));

        System.out.println(posixCatalogIOManager.grep(path, "HG01880", 0, true).getContent());
    }

}