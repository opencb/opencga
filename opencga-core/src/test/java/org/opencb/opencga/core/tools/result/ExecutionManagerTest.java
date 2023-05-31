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

package org.opencb.opencga.core.tools.result;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class ExecutionManagerTest {

    private ExecutionResultManager erm;
    private static Path rootDir;

    @BeforeClass
    public static void beforeClass() throws IOException {
        rootDir = Paths.get("target/test-data", "junit-opencga-storage-" +
                new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS").format(new Date()));
        Files.createDirectories(rootDir);
    }

    @Before
    public void setUp() throws Exception {
        erm = new ExecutionResultManager("myTest", rootDir);
        erm.setMonitorThreadPeriod(1000);
        erm.init(new ObjectMap(), new ObjectMap());
        erm.setSteps(Arrays.asList("step1", "step2"));
    }

    @After
    public void tearDown() throws Exception {
        erm.close();
    }

    @Test
    public void testCheckStep() throws ToolException {
        assertTrue(erm.checkStep("step1"));
        assertTrue(erm.checkStep("step2"));
        assertFalse(erm.checkStep("step1"));
    }

    @Test(expected = ToolException.class)
    public void testCheckStepWrong() throws ToolException {
        erm.checkStep("OtherStep");
     }

    @Test
    public void checkMonitorThread() throws InterruptedException, ToolException {
        Date pre = erm.read().getStatus().getDate();
        Thread.sleep(2000);
        Date post = erm.read().getStatus().getDate();
        assertNotEquals(pre, post);
    }

}