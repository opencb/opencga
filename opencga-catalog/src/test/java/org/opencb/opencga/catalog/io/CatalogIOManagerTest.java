/*
 * Copyright 2015-2020 OpenCB
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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class CatalogIOManagerTest {

    private static CatalogIOManager ioManager;
    private static Path tmpOutdir;

    @BeforeClass
    public static void beforeClass() {
        tmpOutdir = Paths.get("target", "test-data", "opencga-" + PosixIOManagerTest.class.getSimpleName() + "-" + TimeUtils.getTimeMillis(),
                "sessions");
    }

    @Before
    public void before() throws Exception {
        if (Files.exists(tmpOutdir)) {
            IOUtils.deleteDirectory(tmpOutdir);
        }
        Files.createDirectories(tmpOutdir);

        Configuration configuration = Configuration.load(PosixIOManagerTest.class.getResource("/configuration-test.yml").openStream());
        configuration.setWorkspace(tmpOutdir.toUri().toString());
        ioManager = new CatalogIOManager(configuration);
        ioManager.createDefaultOpenCGAFolders();
    }

    @Test
    public void testCreateAccount() throws Exception {
        String userId = "imedina";
        URI userUri = ioManager.createUser(userId);

        Path userPath = Paths.get(userUri);
        assertTrue(Files.exists(userPath));
        assertEquals(tmpOutdir.resolve("users/" + userId).toAbsolutePath().toString(), userUri.getPath());

        ioManager.deleteUser(userId);
        assertFalse(Files.exists(userPath));
    }

    @Test
    public void testCreateStudy() throws Exception {
        String userId = "imedina";
        String projectId = "1000g";

        Path userPath = Paths.get(ioManager.createUser(userId));

        Path projectPath = Paths.get(ioManager.createProject(userId, projectId));
        assertTrue(Files.exists(projectPath));
        assertEquals(userPath.toString() + "/projects/" + projectId, projectPath.toString());

        Path studyPath = Paths.get(ioManager.createStudy(userId, projectId, "phase1"));
        assertTrue(Files.exists(studyPath));
        assertEquals(projectPath.toString() + "/phase1", studyPath.toString());
    }
}