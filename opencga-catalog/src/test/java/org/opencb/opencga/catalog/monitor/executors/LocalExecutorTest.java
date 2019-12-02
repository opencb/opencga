package org.opencb.opencga.catalog.monitor.executors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.Job;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

public class LocalExecutorTest {
    private LocalExecutor localExecutor;
    private Path rootDir;

    @Before
    public void setUp() throws Exception {
        localExecutor = new LocalExecutor(new Execution().setOptions(new ObjectMap(LocalExecutor.MAX_CONCURRENT_JOBS, 5)));
        rootDir = Paths.get("target/test-data", "junit-opencga-" +
                new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS").format(new Date()));
        Files.createDirectories(rootDir);
    }


    @Test(timeout = 10000)
    public void test() throws Exception {
        System.out.println(rootDir.toAbsolutePath());
        for (int i = 0; i < 10; i++) {
            localExecutor.execute("jobId-" + i, "echo Hello World", rootDir.resolve("out_" + i + ".txt"), rootDir.resolve("err_" + i + ".txt"));
        }

        for (int i = 0; i < 10; i++) {
            String jobId = "jobId-" + i;
            while(!localExecutor.getStatus(jobId).equals("DONE")) {
                Thread.sleep(1000);
            }
            Assert.assertTrue(Files.exists(rootDir.resolve("out_" + i + ".txt")));
            Assert.assertEquals(Collections.singletonList("Hello World"), Files.readAllLines(rootDir.resolve("out_" + i + ".txt")));
            Assert.assertTrue(Files.exists(rootDir.resolve("err_" + i + ".txt")));
            Assert.assertEquals(Collections.emptyList(), Files.readAllLines(rootDir.resolve("err_" + i + ".txt")));
        }


    }
}