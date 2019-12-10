package org.opencb.opencga.core.tools.result;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exception.ToolException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.*;

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