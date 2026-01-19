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
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.exec.Command;
import org.opencb.commons.exec.RunnableProcess;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.ExecutionRequirements;
import org.opencb.opencga.core.config.ExecutionRequirementsFactor;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
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

    @Test
    public void testContainerSync() throws Exception {

        Path outDir = getOutDir();

        int sleep = 2;
        Future<RunnableProcess.Status> runMain = runMainCommand(sleep, outDir);
        Future<RunnableProcess.Status> runDind = runDind(outDir);

        assertEquals(RunnableProcess.Status.DONE, runDind.get());
        assertEquals(RunnableProcess.Status.DONE, runMain.get());
    }

    @Test
    public void testContainerInterrupt() throws Exception {

        Path outDir = getOutDir();

        int sleep = 10;
        Future<RunnableProcess.Status> runMain = runMainCommand(sleep, outDir);
        Future<RunnableProcess.Status> runDind = runDind(outDir);

        Thread.sleep(2000);

        // Read PID and kill -15
        kill(outDir, "PID", "-15");

        assertEquals(RunnableProcess.Status.DONE, runDind.get());
        assertEquals(RunnableProcess.Status.ERROR, runMain.get());

        assertEquals(true, Files.exists(outDir.resolve("done")));
        assertEquals(false, Files.exists(outDir.resolve("INTERRUPTED")));
    }

    @Test
    public void testContainerKilled() throws Exception {

        Path outDir = getOutDir();

        int sleep = 10;
        Future<RunnableProcess.Status> runMain = runMainCommand(sleep, outDir);
        Future<RunnableProcess.Status> runDind = runDind(outDir);

        Thread.sleep(2000);

        // Read PID and kill -9
        kill(outDir, "PID", "-9");


        assertEquals(RunnableProcess.Status.DONE, runDind.get());
        assertEquals(RunnableProcess.Status.ERROR, runMain.get());

        assertEquals(true, Files.exists(outDir.resolve("done")));
        assertEquals(false, Files.exists(outDir.resolve("INTERRUPTED")));
    }

    @Test
    public void testContainerKilledParent() throws Exception {
        Path outDir = getOutDir();

        int sleep = 10;
        Command mainCommand = mainCommand(sleep, outDir);
        Future<RunnableProcess.Status> runMain = mainCommand.run(true);
        Future<RunnableProcess.Status> runDind = runDind(outDir);

        Thread.sleep(2000);

        // Read PID and kill -9
        kill(outDir, "PID_MAIN", "-9");

        // Heartbeat should detect that the parent is dead and exit
        assertEquals(RunnableProcess.Status.ERROR, runDind.get());
        assertEquals(RunnableProcess.Status.ERROR, runMain.get());

        // Ensure no "done" file is created
        assertEquals(false, Files.exists(outDir.resolve("done")));
        // Ensure INTERRUPTED file is not created, as the dind process was killed
        assertEquals(false, Files.exists(outDir.resolve("INTERRUPTED")));
    }


    @Test
    public void testContainerTermParent() throws Exception {
        Path outDir = getOutDir();

        int sleep = 10;
        Command mainCommand = mainCommand(sleep, outDir);
        Future<RunnableProcess.Status> runMain = mainCommand.run(true);
        Future<RunnableProcess.Status> runDind = runDind(outDir);

        Thread.sleep(2000);

        // Read PID and kill -15
        kill(outDir, "PID_MAIN", "-15");

        // Main process should terminate gracefully, dind should detect the "done" file and exit gracefully
        assertEquals(RunnableProcess.Status.DONE, runDind.get());
        assertEquals(RunnableProcess.Status.ERROR, runMain.get());

        assertEquals(true, Files.exists(outDir.resolve("done")));
        // Ensure INTERRUPTED file is created as the dind process detected the termination
        assertEquals(true, Files.exists(outDir.resolve("INTERRUPTED")));
    }

    @Test
    public void testContainerMainNotStarted() throws Exception {
        Path outDir = getOutDir();

        Future<RunnableProcess.Status> runDind = runDind(outDir);
        assertEquals(RunnableProcess.Status.ERROR, runDind.get());
    }

    private static void kill(Path outDir, String pidFile, String signal) throws IOException {
        Path pid = outDir.resolve(pidFile);
        List<String> pidLines = Files.readAllLines(pid);
        String pidStr = pidLines.get(0).trim();
        System.out.println("Killing PID " + pidStr);
        String[] killArgs = {"/bin/bash", "-c", "kill " + signal + " " + pidStr};
        Command killCommand = new Command(killArgs, Collections.emptyList());
        killCommand.run();
    }


    private static Future<RunnableProcess.Status> runDind(Path outDir) {
        return dindCommand(10, outDir).run(true);
    }

    private static Command dindCommand(int sleep, Path outDir) {
        String dind = K8SExecutor.getDindCommandline("sleep " + sleep * 10 + " &", true, 4, 2);
        dind =  "cd " + outDir + ";\n" + dind.replace("/usr/share/pod/", outDir.toAbsolutePath() + "/");
        String[] commandDindArgs = {"/bin/bash", "-c", ""};
        Command commandDind = new Command(commandDindArgs, Collections.emptyList());
        commandDindArgs[2] = dind;
        return commandDind;
    }

    private static Future<RunnableProcess.Status> runMainCommand(int sleep, Path outDir) {
        return mainCommand(sleep, outDir).run(true);
    }

    private static Command mainCommand(int sleep, Path outDir) {
        String commandLine = K8SExecutor.getCommandLine("bash -c 'for i in {1.."+ sleep +"} ; do echo $i ; sleep 1; done ; echo END '",
                outDir.resolve("stdout.txt"), outDir.resolve("stderr.txt"), true, true);
        commandLine =  "cd " + outDir + ";\n" + commandLine.replace("/usr/share/pod/", outDir.toAbsolutePath() + "/");
//        System.out.println("commandLine = " + commandLine);
        String[] commandMainArgs = {"/bin/bash", "-c", ""};
        Command commandMain = new Command(commandMainArgs, Collections.emptyList());
        commandMainArgs[2] =  commandLine;
        return commandMain;
    }

    private static Path getOutDir() throws IOException {
        Path outDir = Paths.get("target/test-data").resolve("junit_k8sexecutor_" + TimeUtils.getTimeMillis() + RandomStringUtils.randomAlphabetic(10));
        Files.createDirectories(outDir);
        return outDir;
    }

}
