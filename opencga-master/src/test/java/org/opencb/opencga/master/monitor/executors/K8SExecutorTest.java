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
