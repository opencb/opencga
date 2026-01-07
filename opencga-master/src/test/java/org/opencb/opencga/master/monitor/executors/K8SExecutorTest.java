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

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.ExecutionRequirements;
import org.opencb.opencga.core.config.ExecutionRequirementsFactor;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(ShortTests.class)
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


    @Test
    public void testConfigureJavaHeap() {
        // Build a minimal Job object with required type set to NATIVE_TOOL (so heap is computed from memoryBytes).
        Job job = new Job();
        job.setType(JobType.NATIVE_TOOL);
        job.setTool(new ToolInfo().setMinimumRequirements(new MinimumRequirements()));

        // Memory request: 8Gi (binary). K8SExecutor uses Quantity.getNumericalAmount().longValue() as "bytes".
        ResourceRequirements req8Gi = new ResourceRequirementsBuilder()
                .addToRequests("memory", new Quantity("8Gi"))
                .build();

        // Case 1: offheap=5% but min 300Mi, overhead fixed 300Mi.
        ObjectMap options = new ObjectMap()
                .append(K8SExecutor.K8S_JAVA_OFFHEAP, "5%")
                .append(K8SExecutor.K8S_MEMORY_OVERHEAD, "300Mi");

        List<EnvVar> envVars =
                K8SExecutor.configureJavaHeap(job, req8Gi, options);

        Map<String, String> env = envVars.stream()
                .collect(Collectors.toMap(EnvVar::getName,
                        EnvVar::getValue));

        // offheap = 5% of 8Gi = 409.6Mi -> should be rendered in JAVA_STYLE (Mi suffix) and used for MaxDirectMemorySize.
        assertEquals("410m", env.get("JAVA_OFF_HEAP"));

        // heap = 8Gi - 410Mi - 300Mi = 7483Mi (in base-2 MiB units with truncation).
        assertEquals("7482m", env.get("JAVA_HEAP"));

        // Case 2: offheap percent below 300Mi should clamp to 300Mi.
        ResourceRequirements req4Gi = new ResourceRequirementsBuilder()
                .addToRequests("memory", new Quantity("4Gi"))
                .build();

        ObjectMap optionsMinClamp = new ObjectMap()
                .append(K8SExecutor.K8S_JAVA_OFFHEAP, "5%")     // 5% of 4Gi = 204.8Mi -> clamp to 300Mi
                .append(K8SExecutor.K8S_MEMORY_OVERHEAD, "300Mi");

        List<EnvVar> envVarsClamp =
                K8SExecutor.configureJavaHeap(job, req4Gi, optionsMinClamp);

        Map<String, String> envClamp = envVarsClamp.stream()
                .collect(Collectors.toMap(EnvVar::getName,
                        EnvVar::getValue));

        assertEquals("300m", envClamp.get("JAVA_OFF_HEAP"));
        // heap = 4Gi - 300Mi - 300Mi = 3496Mi.
        assertEquals("3496m", envClamp.get("JAVA_HEAP"));

        // Case 3: explicit heap and offheap should be respected (heap not recomputed).
        ObjectMap optionsExplicit = new ObjectMap()
                .append(K8SExecutor.K8S_JAVA_HEAP, "2Gi")
                .append(K8SExecutor.K8S_JAVA_OFFHEAP, "512Mi")
                .append(K8SExecutor.K8S_MEMORY_OVERHEAD, "300Mi");

        List<EnvVar> envVarsExplicit =
                K8SExecutor.configureJavaHeap(job, req8Gi, optionsExplicit);

        Map<String, String> envExplicit = envVarsExplicit.stream()
                .collect(Collectors.toMap(EnvVar::getName,
                        EnvVar::getValue));

        assertEquals("512m", envExplicit.get("JAVA_OFF_HEAP"));
        assertEquals("2g", envExplicit.get("JAVA_HEAP"));
    }

    @Test
    public void testGetResourcesAppliesDefaultsAndFactor() {
        ExecutionRequirements defaults = new ExecutionRequirements();
        defaults.setCpu(2);
        defaults.setMemory("1024Mi");

        ExecutionRequirementsFactor factor = new ExecutionRequirementsFactor();
        factor.setCpu((float) 1.5);
        factor.setMemory((float) 2.0);

        ResourceRequirements resources = K8SExecutor.getResources(null, defaults, factor);

        assertNotNull(resources);
        assertNotNull(resources.getRequests());
        assertNotNull(resources.getLimits());

        Quantity reqCpu = resources.getRequests().get("cpu");
        Quantity limCpu = resources.getLimits().get("cpu");
        assertNotNull(reqCpu);
        assertNotNull(limCpu);
        assertEquals("cpu request", new BigDecimal("3.0"), reqCpu.getNumericalAmount());
        assertEquals("cpu limit", new BigDecimal("3.0"), limCpu.getNumericalAmount());

        Quantity reqMem = resources.getRequests().get("memory");
        Quantity limMem = resources.getLimits().get("memory");
        assertNotNull(reqMem);
        assertNotNull(limMem);
        assertEquals("memory request", new BigDecimal("2147483648.0"), reqMem.getNumericalAmount());
        assertEquals("memory limit", new BigDecimal("2147483648.0"), limMem.getNumericalAmount());

        assertEquals("memory request format", "2.0Gi", reqMem.getAmount() + reqMem.getFormat());
        assertEquals("memory limit format", "2.0Gi", limMem.getAmount() + reqMem.getFormat());
    }

    @Test
    public void testGetResourcesUsesMinimumRequirementsAndFactor() {
        MinimumRequirements min = new MinimumRequirements("1", "512Mi", null, null, null);
        ExecutionRequirements defaults = new ExecutionRequirements(8, "8Gi");
        ExecutionRequirementsFactor factor = new ExecutionRequirementsFactor(2.0f, 3.0f);

        ResourceRequirements resources = K8SExecutor.getResources(min, defaults, factor);

        Quantity reqCpu = resources.getRequests().get("cpu");
        Quantity reqMem = resources.getRequests().get("memory");

        assertEquals(new BigDecimal("2.0"), reqCpu.getNumericalAmount());
        assertEquals(new BigDecimal("1610612736.0"), reqMem.getNumericalAmount()); // 512Mi \* 3
        assertEquals("1536.0Mi", reqMem.getAmount()+reqMem.getFormat()); // 512Mi \* 3
    }

}
