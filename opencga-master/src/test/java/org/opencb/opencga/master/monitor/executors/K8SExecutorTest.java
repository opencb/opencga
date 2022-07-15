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
        assertEquals("opencga-job-study-really-complicated-j-b-2---id-80a79e",
                K8SExecutor.buildJobName("user@project:study", "really_Complicated J@b 2£$ ID"));

        String jobName = K8SExecutor.buildJobName("user@project:study", "really_Complicated and extra super duper large job name for a simple task 20210323122209");
        String expected = "opencga-job-study-really-co-89b6a8--simple-task-20210323122209";
        assertEquals(expected, jobName);

        assertEquals("opencga-job-my-job-1-92b223", K8SExecutor.buildJobName(null, "my job 1"));
        assertEquals("opencga-job-my-job-1-f3eb13", K8SExecutor.buildJobName(null, "my_job_1"));
        assertEquals("opencga-job-my-job-1-672a51", K8SExecutor.buildJobName(null, "my:job:1"));
        assertEquals("opencga-job-my-job-1-94caca", K8SExecutor.buildJobName(null, "My-job-1"));
        assertEquals("opencga-job-my-job-1", K8SExecutor.buildJobName(null, "my-job-1"));

        assertEquals("opencga-job-study-my-job-1-39dd81", K8SExecutor.buildJobName("user@project:study", "my job 1"));
        assertEquals("opencga-job-study-my-job-1-0bb01d", K8SExecutor.buildJobName("user@project:study", "my_job_1"));
        assertEquals("opencga-job-study-my-job-1-75954c", K8SExecutor.buildJobName("user@project:study", "my:job:1"));
        assertEquals("opencga-job-study-my-job-1-d95244", K8SExecutor.buildJobName("user@project:study", "My-job-1"));
        assertEquals("opencga-job-study-my-job-1-ecd290", K8SExecutor.buildJobName("user@project:study", "my-job-1"));
        assertEquals("opencga-job-study-my-job-1-354a5a", K8SExecutor.buildJobName("user2@project:study", "my-job-1"));
        assertEquals("opencga-job-study-my-job-1-da217f", K8SExecutor.buildJobName("user@project2:study", "my-job-1"));

        assertEquals("opencga-job-abcdefghijklmnopqrstuvwxyz12345678900987654321", K8SExecutor.buildJobName(null, "abcdefghijklmnopqrstuvwxyz12345678900987654321"));
        assertEquals("opencga-job-abcdefghijklmno-cb3af7-tuvwxyz12345678900987654321", K8SExecutor.buildJobName(null, "ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678900987654321"));
        assertEquals("opencga-job-abcdefghijklmno-47a86e-stuvwxyz1234567890098765432", K8SExecutor.buildJobName(null, "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890098765432"));
        assertEquals("opencga-job-abcdefghijklmno-9c2366-rstuvwxyz123456789009876543", K8SExecutor.buildJobName(null, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789009876543"));
        assertEquals("opencga-job-abcdefghijklmnopqrstuvwxyz12345678900987654-890ca6", K8SExecutor.buildJobName(null, "ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678900987654"));
        assertEquals("opencga-job-abcdefghijklmnopqrstuvwxyz1234567890-016fd6", K8SExecutor.buildJobName(null, "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"));

        assertEquals("opencga-job-s-abcdefghijklmnopqrstuvwxyz12345678900987654321", K8SExecutor.buildJobName("s", "abcdefghijklmnopqrstuvwxyz12345678900987654321"));
        assertEquals("opencga-job-s-abcdefghijklm-adb5ea-tuvwxyz12345678900987654321", K8SExecutor.buildJobName("s", "ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678900987654321"));
        assertEquals("opencga-job-s-abcdefghijklm-2cc773-stuvwxyz1234567890098765432", K8SExecutor.buildJobName("s", "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890098765432"));
        assertEquals("opencga-job-s-abcdefghijklm-6fb1b7-rstuvwxyz123456789009876543", K8SExecutor.buildJobName("s", "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789009876543"));
        assertEquals("opencga-job-s-abcdefghijklmnopqrstuvwxyz123456789009876-a64d4a", K8SExecutor.buildJobName("s", "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789009876"));
        assertEquals("opencga-job-s-abcdefghijklmnopqrstuvwxyz1234567890-72eaa2", K8SExecutor.buildJobName("s", "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"));
    }

}
