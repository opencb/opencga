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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void testCreateStudy() throws Exception {
        String organizationId = "opencb";
        ioManager.createOrganization(organizationId);
        String projectId = "1000g";

        Path projectPath = Paths.get(ioManager.createProject(organizationId, projectId));
        assertTrue(Files.exists(projectPath));
        assertEquals(tmpOutdir.resolve("orgs").resolve(organizationId).resolve("projects").resolve(projectId).toAbsolutePath().toString(), projectPath.toString());

        Path studyPath = Paths.get(ioManager.createStudy(organizationId, projectId, "phase1"));
        assertTrue(Files.exists(studyPath));
        assertEquals(projectPath.resolve("phase1").toString(), studyPath.toString());
    }
}