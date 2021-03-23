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

import static org.junit.Assert.assertEquals;

public class K8SExecutorTest {

    @Test
    public void testBuildJobName() {
        assertEquals("opencga-job-really-complicated-j-b-2---id-e8b6f0",
                K8SExecutor.buildJobName("really_Complicated J@b 2£$ ID"));

        String jobName = K8SExecutor.buildJobName("really_Complicated and extra super duper large job name for a simple task 20210323122209");
        String expected = "opencga-job-really-complica-1d9128--simple-task-20210323122209";
        assertEquals(expected, jobName);

        assertEquals("opencga-job-my-job-1-92b223", K8SExecutor.buildJobName("my job 1"));
        assertEquals("opencga-job-my-job-1-f3eb13", K8SExecutor.buildJobName("my_job_1"));
        assertEquals("opencga-job-my-job-1-672a51", K8SExecutor.buildJobName("my:job:1"));
        assertEquals("opencga-job-my-job-1-94caca", K8SExecutor.buildJobName("My-job-1"));
        assertEquals("opencga-job-my-job-1", K8SExecutor.buildJobName("my-job-1"));

        assertEquals("opencga-job-abcdefghijklmnopqrstuvwxyz12345678900987654321", K8SExecutor.buildJobName("abcdefghijklmnopqrstuvwxyz12345678900987654321"));
        assertEquals("opencga-job-abcdefghijklmno-cb3af7-tuvwxyz12345678900987654321", K8SExecutor.buildJobName("ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678900987654321"));
        assertEquals("opencga-job-abcdefghijklmno-47a86e-stuvwxyz1234567890098765432", K8SExecutor.buildJobName("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890098765432"));
        assertEquals("opencga-job-abcdefghijklmno-9c2366-rstuvwxyz123456789009876543", K8SExecutor.buildJobName("ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789009876543"));
        assertEquals("opencga-job-abcdefghijklmnopqrstuvwxyz12345678900987654-890ca6", K8SExecutor.buildJobName("ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678900987654"));
        assertEquals("opencga-job-abcdefghijklmnopqrstuvwxyz1234567890-016fd6", K8SExecutor.buildJobName("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"));
    }

}
