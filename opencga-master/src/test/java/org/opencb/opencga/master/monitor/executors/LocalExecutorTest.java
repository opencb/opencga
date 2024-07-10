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

package org.opencb.opencga.master.monitor.executors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ShortTests.class)
public class LocalExecutorTest {
    public static final int MAX_CONCURRENT_JOBS = 5;
    private LocalExecutor localExecutor;
    private Path rootDir;

    @Before
    public void setUp() throws Exception {
        localExecutor = new LocalExecutor(new Execution().setOptions(new ObjectMap(LocalExecutor.MAX_CONCURRENT_JOBS, MAX_CONCURRENT_JOBS)));
        rootDir = Paths.get("target/test-data", "junit-opencga-" +
                new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS").format(new Date()));
        Files.createDirectories(rootDir);
    }


    @Test(timeout = 10000)
    public void test() throws Exception {
        System.out.println(rootDir.toAbsolutePath());
        for (int i = 0; i < 10; i++) {
            localExecutor.execute("jobId-" + i, "default", "echo Hello World", rootDir.resolve("out_" + i + ".txt"), rootDir.resolve("err_" + i + ".txt"));
        }

        for (int i = 0; i < 10; i++) {
            String jobId = "jobId-" + i;
            while(!localExecutor.getStatus(jobId).equals("DONE")) {
                Thread.sleep(1000);
            }
            Assert.assertTrue(Files.exists(rootDir.resolve("out_" + i + ".txt")));
            assertEquals(Collections.singletonList("Hello World"), Files.readAllLines(rootDir.resolve("out_" + i + ".txt")));
            Assert.assertTrue(Files.exists(rootDir.resolve("err_" + i + ".txt")));
            assertEquals(Collections.emptyList(), Files.readAllLines(rootDir.resolve("err_" + i + ".txt")));
        }
    }

    @Test(timeout = 10000)
    public void testKill() throws Exception {
        System.out.println(rootDir.toAbsolutePath());
        for (int i = 0; i < 10; i++) {
//            System.out.println("Submitting job " + i);
            localExecutor.execute("jobId-" + i, "default", "sleep 20", rootDir.resolve("out_" + i + ".txt"), rootDir.resolve("err_" + i + ".txt"));
        }

        // Allow some time for the jobs to start
        Thread.sleep(50);

        for (int i = 0; i < 10; i++) {
            String jobId = "jobId-" + i;
//            System.out.println("Checking status of job " + jobId);
            if (i < MAX_CONCURRENT_JOBS) {
                assertEquals(jobId, "RUNNING", localExecutor.getStatus(jobId));
            } else {
                assertEquals(jobId, "QUEUED", localExecutor.getStatus(jobId));
            }
        }
        for (int i = 0; i < 10; i++) {
            String jobId = "jobId-" + i;
//            System.out.println("Checking status of job " + jobId);
            while(!localExecutor.getStatus(jobId).equals("RUNNING")) {
                Thread.sleep(10);
            }
            assertEquals("RUNNING", localExecutor.getStatus(jobId));
            assertTrue(localExecutor.kill(jobId));
        }
    }

}