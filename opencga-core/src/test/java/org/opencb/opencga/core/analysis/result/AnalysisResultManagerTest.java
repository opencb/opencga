package org.opencb.opencga.core.analysis.result;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exception.AnalysisException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.*;

public class AnalysisResultManagerTest {

    private AnalysisResultManager arm;
    private static Path rootDir;

    @BeforeClass
    public static void beforeClass() throws IOException {
        rootDir = Paths.get("target/test-data", "junit-opencga-storage-" +
                new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS").format(new Date()));
        Files.createDirectories(rootDir);
    }

    @Before
    public void setUp() throws Exception {
        arm = new AnalysisResultManager("myTest", rootDir);
        arm.setMonitorThreadPeriod(1000);
        arm.init(new ObjectMap(), new ObjectMap());
        arm.setSteps(Arrays.asList("step1", "step2"));
    }

    @After
    public void tearDown() throws Exception {
        arm.close();
    }

    @Test
    public void testReadWrite() throws Exception {
        arm.checkStep("step1");

        Path file = rootDir.resolve("file1.txt");
        Files.createFile(file);
        arm.addFile(file.toAbsolutePath(), FileResult.FileType.TAB_SEPARATED);
    }

    @Test
    public void testCheckStep() throws AnalysisException {
        assertTrue(arm.checkStep("step1"));
        assertTrue(arm.checkStep("step2"));
        assertFalse(arm.checkStep("step1"));
    }

    @Test(expected = AnalysisException.class)
    public void testCheckStepWrong() throws AnalysisException {
        arm.checkStep("OtherStep");
     }

    @Test
    public void checkMonitorThread() throws InterruptedException, AnalysisException {
        Date pre = arm.read().getStatus().getDate();
        Thread.sleep(2000);
        Date post = arm.read().getStatus().getDate();
        assertNotEquals(pre, post);
    }

}