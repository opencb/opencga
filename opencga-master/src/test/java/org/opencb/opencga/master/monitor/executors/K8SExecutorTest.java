package org.opencb.opencga.master.monitor.executors;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class K8SExecutorTest {

    @Test
    public void testBuildJobName() {
        assertEquals("opencga-job-really-complicated.j.b.2...id", K8SExecutor.buildJobName("really_Complicated J@b 2£$ ID"));
    }

}