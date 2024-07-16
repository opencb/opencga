package org.opencb.opencga.analysis.tools;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.nextflow.NextFlowRunParams;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class NextFlowExecutorTest {

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource(false);

    @Test
    public void myTest() throws ToolException, IOException {
        Path outDir = Paths.get(opencga.createTmpOutdir("_nextflow"));

        StopWatch stopWatch = StopWatch.createStarted();
        NextFlowExecutor nextFlowExecutorTest = new NextFlowExecutor();
        NextFlowRunParams runParams = new NextFlowRunParams("myId", 2);
        nextFlowExecutorTest.setUp(opencga.getOpencgaHome().toString(), runParams.toObjectMap(), outDir, opencga.getAdminToken());
        nextFlowExecutorTest.start();
        System.out.println(stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

}
