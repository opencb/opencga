package org.opencb.opencga.master.monitor.executors;

import org.junit.Test;
import org.opencb.opencga.core.common.TimeUtils;

import static org.junit.Assert.assertEquals;

public class K8SExecutorTest {

    @Test
    public void testBuildJobName() {
        assertEquals("opencga-job-really-complicated-j-b-2---id",
                K8SExecutor.buildJobName("really_Complicated J@b 2£$ ID"));
        String time = TimeUtils.getTime();

        String jobName = K8SExecutor.buildJobName("really_Complicated and extra super duper large job name for a simple task " + time);
        String expected = "opencga-job-really-complicated--r-a-simple-task-" + time;
        assertEquals(expected, jobName);
        assertEquals(expected.length(), jobName.length());
    }

}
