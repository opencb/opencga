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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpecBuilder;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class K8SExecutor implements BatchExecutor {

    public static final String K8S_MASTER_NODE = "k8s.masterUrl";
    public static final String K8S_CLIENT_TIMEOUT = "k8s.clientTimeout";
    public static final String K8S_IMAGE_NAME = "k8s.imageName";
    public static final String K8S_IMAGE_PULL_POLICY = "k8s.imagePullPolicy";
    public static final String K8S_IMAGE_PULL_SECRETS = "k8s.imagePullSecrets";
    public static final String K8S_TTL_SECONDS_AFTER_FINISHED = "k8s.ttlSecondsAfterFinished";
    public static final String K8S_TERMINATION_GRACE_PERIOD_SECONDS = "k8s.terminationGracePeriodSeconds";
    public static final String K8S_LOG_TO_STDOUT = "k8s.logToStdout";
    public static final String K8S_DIND_ROOTLESS = "k8s.dind.rootless";
    public static final String K8S_DIND_IMAGE_NAME = "k8s.dind.imageName";
    public static final String K8S_RUNTIME_CLASS = "k8s.runtimeClass";
    public static final String K8S_JAVA_HEAP = "k8s.javaHeap";
    public static final String K8S_JAVA_OFFHEAP = "k8s.javaOffHeap";
    public static final String K8S_MEMORY_OVERHEAD = "k8s.memoryOverhead";
    public static final String K8S_ENVS = "k8s.envs";
    public static final String K8S_NAMESPACE = "k8s.namespace";
    public static final String K8S_VOLUME_MOUNTS = "k8s.volumeMounts";
    public static final String K8S_VOLUMES = "k8s.volumes";
    public static final String K8S_NODE_SELECTOR = "k8s.nodeSelector";
    public static final String K8S_TOLERATIONS = "k8s.tolerations";
    public static final String K8S_SECURITY_CONTEXT = "k8s.securityContext";
    public static final String K8S_POD_SECURITY_CONTEXT = "k8s.podSecurityContext";
    public static final EnvVar DOCKER_HOST = new EnvVar("DOCKER_HOST", "tcp://localhost:2375", null);
    public static final int DEFAULT_TIMEOUT = 30000; // in ms

    private static final Volume DOCKER_GRAPH_STORAGE_VOLUME = new VolumeBuilder()
            .withName("docker-graph-storage")
            .withEmptyDir(new EmptyDirVolumeSource())
            .build();
    private static final VolumeMount DOCKER_GRAPH_VOLUMEMOUNT = new VolumeMountBuilder()
            .withName("docker-graph-storage")
            .withMountPath("/var/lib/docker")
            .build();

    // Use tmp-pod volume to communicate the dind container when the main job container has finished.
    // Otherwise, this container would be running forever
    private static final Volume TMP_POD_VOLUME = new VolumeBuilder()
            .withName("tmp-pod")
            .withEmptyDir(new EmptyDirVolumeSource())
            .build();
    private static final VolumeMount TMP_POD_VOLUMEMOUNT = new VolumeMountBuilder()
            .withName("tmp-pod")
            .withMountPath("/usr/share/pod")
            .build();
    private static final String DIND_DONE_FILE = "/usr/share/pod/done";
    public static final String JOB_NAME = "job-name";

    private final String namespace;
    private final String imageName;
    private final List<VolumeMount> volumeMounts;
    private final List<Volume> volumes;
    private final Map<String, String> nodeSelector;
    private final List<Toleration> tolerations;
    private final SecurityContext securityContext;
    private final PodSecurityContext podSecurityContext;
    private final List<EnvVar> envVars;
    private final Config k8sConfig;
    private final String runtimeClass;
    private final Container dockerDaemonSidecar;
    private final KubernetesClient kubernetesClient;
    private static Logger logger = LoggerFactory.getLogger(K8SExecutor.class);

    private final Map<String, Pair<Instant, String>> jobStatusCache = new ConcurrentHashMap<>();
    private final Watch podsWatcher;
    private final Watch jobsWatcher;
    private final String imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;
    private final int ttlSecondsAfterFinished;
    private final boolean logToStdout;
    private final ExecutionRequirements defaultRequirements;
    private final ObjectMap options;
    private final boolean isGpu;
    private long terminationGracePeriodSeconds;
    private final ExecutionRequirementsFactor executionFactor;

    public K8SExecutor(Configuration configuration, ExecutionQueue executionQueue) {
        Execution execution = configuration.getAnalysis().getExecution();
        options = execution.getOptions();
        options.putAll(executionQueue.getOptions());
        this.isGpu = executionQueue.getProcessorType() == ExecutionQueue.ProcessorType.GPU;
        String k8sClusterMaster = options.getString(K8S_MASTER_NODE);
        this.namespace = options.getString(K8S_NAMESPACE);
        this.imageName = options.getString(K8S_IMAGE_NAME);
        this.volumeMounts = buildVolumeMounts(options.getList(K8S_VOLUME_MOUNTS));
        this.volumes = buildVolumes(options.getList(K8S_VOLUMES));
        this.tolerations = buildTolelrations(options.getList(K8S_TOLERATIONS));
        this.k8sConfig = new ConfigBuilder()
                .withMasterUrl(k8sClusterMaster)
                // Connection timeout in ms (0 for no timeout)
                .withConnectionTimeout(options.getInt(K8S_CLIENT_TIMEOUT, DEFAULT_TIMEOUT))
                // Read timeout in ms
                .withRequestTimeout(options.getInt(K8S_CLIENT_TIMEOUT, 30000))
                .build();
        this.kubernetesClient = new DefaultKubernetesClient(k8sConfig).inNamespace(namespace);
        this.imagePullPolicy = options.getString(K8S_IMAGE_PULL_POLICY, "IfNotPresent");
        this.imagePullSecrets = buildLocalObjectReference(options.get(K8S_IMAGE_PULL_SECRETS));
        this.ttlSecondsAfterFinished = options.getInt(K8S_TTL_SECONDS_AFTER_FINISHED, 3600);
        this.terminationGracePeriodSeconds = options.getInt(K8S_TERMINATION_GRACE_PERIOD_SECONDS, 5 * 60);
        this.logToStdout = options.getBoolean(K8S_LOG_TO_STDOUT, true);
        this.executionFactor = execution.getRequirementsFactor();
        this.defaultRequirements = execution.getDefaultRequirements();

        this.nodeSelector = new HashMap<>();
        // Override node selector if defined in execution options
        this.nodeSelector.putAll(getMap(options, K8S_NODE_SELECTOR));


        if (options.containsKey(K8S_SECURITY_CONTEXT)) {
            securityContext = buildObject(options.get(K8S_SECURITY_CONTEXT), SecurityContext.class);
        } else {
            securityContext = new SecurityContextBuilder()
                    .withRunAsUser(1001L)
                    .withRunAsNonRoot(true)
                    .withReadOnlyRootFilesystem(false)
                    .build();
        }
        if (options.containsKey(K8S_POD_SECURITY_CONTEXT)) {
            podSecurityContext = buildObject(options.get(K8S_POD_SECURITY_CONTEXT), PodSecurityContext.class);
        } else {
            podSecurityContext = new PodSecurityContextBuilder()
                    .withRunAsNonRoot(true)
                    .build();
        }

        envVars = new ArrayList<>();
        envVars.add(DOCKER_HOST);
        Map<String, String> map = getMap(options, K8S_ENVS);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            envVars.add(new EnvVar(entry.getKey(), entry.getValue(), null));
        }

        String scratchDir = configuration.getAnalysis().getScratchDir();
        if (StringUtils.isNotEmpty(scratchDir)) {
            Volume scratchVolume = new VolumeBuilder()
                    .withName("scratch")
                    .withEmptyDir(new EmptyDirVolumeSource())
                    .build();
            volumes.add(scratchVolume);

            VolumeMount scratchVolumemount = new VolumeMountBuilder()
                    .withName("scratch")
                    .withMountPath(scratchDir)
                    .build();
            volumeMounts.add(scratchVolumemount);
        }

        // Initialize GPU configurations
        this.runtimeClass = options.getString(K8S_RUNTIME_CLASS, null);

        this.dockerDaemonSidecar = buildDindSidecar();

        jobsWatcher = getKubernetesClient().batch().jobs().watch(new Watcher<Job>() {
            @Override
            public void eventReceived(Action action, Job k8Job) {
                String k8sJobName = k8Job.getMetadata().getName();
                logger.debug("Received event '{}' from JOB '{}'", action, k8sJobName);
                if (action == Action.DELETED) {
                    jobStatusCache.remove(k8sJobName);
                } else {
                    String status = getStatusFromK8sJob(k8Job, k8sJobName);
                    jobStatusCache.put(k8sJobName, Pair.of(Instant.now(), status));
                }
            }

            @Override
            public void onClose(WatcherException e) {
                if (e != null) {
                    logger.error("Catch exception at jobs watcher", e);
                }
            }
        });

        podsWatcher = getKubernetesClient().pods().watch(new Watcher<Pod>() {
            @Override
            public void eventReceived(Action action, Pod pod) {
                String k8jobName = getJobName(pod);
                logger.debug("Received event '{}' from POD '{}'", action, pod.getMetadata().getName());
                if (StringUtils.isEmpty(k8jobName)) {
                    return;
                }
                if (action == Action.DELETED) {
                    jobStatusCache.remove(k8jobName);
                } else {
                    String status = getStatusFromPod(pod);
                    jobStatusCache.put(k8jobName, Pair.of(Instant.now(), status));
                }
            }

            @Override
            public void onClose(WatcherException e) {
                if (e != null) {
                    logger.error("Catch exception at pods watcher", e);
                }
            }
        });
    }

    private Container buildDindSidecar() {
        String dindImageName = options.getString(K8S_DIND_IMAGE_NAME, isGpu ? "ghcr.io/ehfd/nvidia-dind:latest" : "docker:dind-rootless");
        boolean rootless;
        if (options.containsKey(K8S_DIND_ROOTLESS)) {
            rootless = options.getBoolean(K8S_DIND_ROOTLESS);
        } else {
            rootless = dindImageName.contains("dind-rootless");
        }
        SecurityContext dindSecurityContext;
        if (rootless) {
            dindSecurityContext = new SecurityContextBuilder()
                    .withRunAsNonRoot(true)
                    .withRunAsUser(1000L)
                    .withPrivileged(true).build();
        } else {
            dindSecurityContext = new SecurityContextBuilder()
                    .withPrivileged(true)
                    .withRunAsNonRoot(false)  // Allow root for Docker daemon
                    .withRunAsUser(0L)        // Explicitly set to root
                    .build();
        }

        String args;
        List<EnvVar> envVars = new ArrayList<>();
        envVars.add(new EnvVar("DOCKER_TLS_CERTDIR", "", null));

        if (isGpu) {
            envVars.add(new EnvVar("NVIDIA_VISIBLE_DEVICES", "all", null));
            envVars.add(new EnvVar("NVIDIA_DRIVER_CAPABILITIES", "all", null));
            args = "# Install NVIDIA Container Runtime if available\n"
                    + "if command -v nvidia-container-runtime >/dev/null 2>&1; then\n"
                    + "  dockerd --host=tcp://0.0.0.0:2375 --host=unix:///var/run/docker.sock"
                    + " --add-runtime=nvidia=/usr/bin/nvidia-container-runtime --default-runtime=nvidia &\n"
                    + "else\n"
                    + "  dockerd --host=tcp://0.0.0.0:2375 --host=unix:///var/run/docker.sock &\n"
                    + "fi\n"
                    + "trap 'echo \"GPU Docker daemon terminated!\"; touch " + DIND_DONE_FILE + "' TERM;\n"
                    + "while ! test -f " + DIND_DONE_FILE + "; do sleep 5; done; exit 0";
        } else {
            args = "dockerd-entrypoint.sh & "
                    // Add trap to capture TERM signal and finish main process
                    + "trap '"
                    + "echo \"Container terminated! ;\n"
                    + "touch " + DIND_DONE_FILE + " ' TERM ;"
                    + "while ! test -f " + DIND_DONE_FILE + "; do sleep 5; done; exit 0";
        }

        return new ContainerBuilder()
                .withName("dind-daemon")
                .withImage(dindImageName)
                .withSecurityContext(dindSecurityContext)
                .withEnv(envVars)
//                .withResources(resources) // TODO: Should we add resources here?
                .withCommand("/bin/sh", "-c")
                .addToArgs(args)
                .addToVolumeMounts(DOCKER_GRAPH_VOLUMEMOUNT)
                .addToVolumeMounts(TMP_POD_VOLUMEMOUNT)
                .addAllToVolumeMounts(volumeMounts)
                .build();
    }

    @Override
    public void close() throws IOException {
        podsWatcher.close();
        jobsWatcher.close();
        kubernetesClient.close();
    }

    @Override
    public void execute(org.opencb.opencga.core.models.job.Job job, String queue, String commandLine, Path stdout, Path stderr)
            throws Exception {
        String jobName = buildJobName(job.getId());
        ResourceRequirements resources = getResources(job.getTool().getMinimumRequirements());
        List<EnvVar> javaHeapEnvVars = configureJavaHeap(job, resources);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder()
                .withTerminationGracePeriodSeconds(terminationGracePeriodSeconds)
                .withImagePullSecrets(imagePullSecrets)
                .addToContainers(new ContainerBuilder()
                        .withName("opencga")
                        .withImage(imageName)
                        .withImagePullPolicy(imagePullPolicy)
                        .withResources(resources)
                        .addAllToEnv(envVars)
                        .addAllToEnv(javaHeapEnvVars)
                        .withCommand("/bin/bash", "-c")
                        .withArgs(getCommandLine(commandLine, stdout, stderr))
                        .withVolumeMounts(volumeMounts)
                        .addToVolumeMounts(TMP_POD_VOLUMEMOUNT)
                        .withSecurityContext(securityContext)
                        .build())
                .withTolerations(tolerations)
                .withRestartPolicy("Never")
                .withNodeSelector(nodeSelector)
                .addAllToVolumes(volumes)
                .addToVolumes(DOCKER_GRAPH_STORAGE_VOLUME)
                .addToVolumes(TMP_POD_VOLUME)
                .withSecurityContext(podSecurityContext);

        if (StringUtils.isNotEmpty(runtimeClass)) {
            logger.info("Setting runtime class: {}", runtimeClass);
            podSpecBuilder.withRuntimeClassName(runtimeClass);
        }

        if (shouldAddDockerDaemon(queue)) {
            podSpecBuilder.addToContainers(dockerDaemonSidecar);
        }

        final io.fabric8.kubernetes.api.model.batch.v1.Job k8sJob = new JobBuilder()
                .withApiVersion("batch/v1")
                .withKind("Job")
                .withMetadata(new ObjectMetaBuilder()
                        .withName(jobName)
                        .withLabels(Collections.singletonMap("opencga", "job"))
                        .build())
                .withSpec(new JobSpecBuilder()
                        .withTtlSecondsAfterFinished(ttlSecondsAfterFinished)
                        .withBackoffLimit(0) // specify the number of retries before considering a Job as failed
                        .withTemplate(new PodTemplateSpecBuilder()
                                .withMetadata(new ObjectMetaBuilder()
                                        // https://github.com/kubernetes/autoscaler/blob/master/
                                        //   cluster-autoscaler/FAQ.md#what-types-of-pods-can-prevent-ca-from-removing-a-node
                                        .addToAnnotations("cluster-autoscaler.kubernetes.io/safe-to-evict", "false")
                                        .build())
                                .withSpec(podSpecBuilder.build())
                                .build())
                        .build()
                ).build();

        jobStatusCache.put(jobName, Pair.of(Instant.now(), Enums.ExecutionStatus.QUEUED));
        getKubernetesClient().batch().v1().jobs().inNamespace(namespace).resource(k8sJob).create();
    }

    private ResourceRequirements getResources(MinimumRequirements minimumRequirements) {
        return getResources(minimumRequirements, defaultRequirements, executionFactor);
    }

    protected static ResourceRequirements getResources(MinimumRequirements minimumRequirements,
                                                       ExecutionRequirements defaultRequirements,
                                                       ExecutionRequirementsFactor executionFactor) {
        if (minimumRequirements == null) {
            minimumRequirements = new MinimumRequirements(
                    String.valueOf(defaultRequirements.getCpu()),
                    defaultRequirements.getMemory(),
                    null,
                    ExecutionQueue.ProcessorType.CPU,
                    null);
        }
        ResourceRequirementsBuilder resources = new ResourceRequirementsBuilder();
        if (StringUtils.isNotEmpty(minimumRequirements.getMemory())) {
            long memoryBytes = IOUtils.fromHumanReadableToByte(minimumRequirements.getMemory());
            Quantity memory = new Quantity(IOUtils.kubernetesStyleByteCount((long) (memoryBytes * executionFactor.getMemory())));
            resources.addToRequests("memory", memory);
            resources.addToLimits("memory", memory);
        }
        if (StringUtils.isNotEmpty(minimumRequirements.getCpu())) {
            double cpuUnits = Double.parseDouble(minimumRequirements.getCpu());
            Quantity cpu = new Quantity(Double.toString(cpuUnits * executionFactor.getCpu()));
            resources.addToRequests("cpu", cpu);
            resources.addToLimits("cpu", cpu);
        }
        return resources.build();
    }

    private List<EnvVar> configureJavaHeap(org.opencb.opencga.core.models.job.Job job, ResourceRequirements requirements) {
        return configureJavaHeap(job, requirements, options);
    }

    protected static List<EnvVar> configureJavaHeap(org.opencb.opencga.core.models.job.Job job, ResourceRequirements requirements,
                                                    ObjectMap options) {
        String javaHeap = options.getString(K8S_JAVA_HEAP);
        String javaOffheap = options.getString(K8S_JAVA_OFFHEAP, "5%");
        String overhead = options.getString(K8S_MEMORY_OVERHEAD, "300Mi");
        Quantity memory = requirements.getRequests().get("memory");
        List<EnvVar> envVars = new ArrayList<>(2);
        long memoryBytes = memory.getNumericalAmount().longValue();
        long overHeadBytes;
        long javaOffheapBytes;

        if (overhead.endsWith("%")) {
            String percentString = overhead.substring(0, overhead.length() - 1);
            try {
                double percent = Double.parseDouble(percentString) / 100.0;
                overHeadBytes = (long) (memoryBytes * percent);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Unable to parse memoryOverhead percentage: " + overhead, e);
            }
        } else {
            overHeadBytes = IOUtils.fromHumanReadableToByte(overhead);
        }

        if (javaOffheap.endsWith("%")) {
            String percentString = javaOffheap.substring(0, javaOffheap.length() - 1);
            try {
                double percent = Double.parseDouble(percentString) / 100.0;
                javaOffheapBytes = (long) (memoryBytes * percent);
                // Set a minimum offheap size of 300MiB
                if (javaOffheapBytes < (300 * 1024 * 1024)) {
                    javaOffheapBytes = 300 * 1024 * 1024;
                }
                javaOffheap = IOUtils.javaStyleByteCount(javaOffheapBytes);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Unable to parse javaOffheap percentage: " + javaOffheap, e);
            }
        } else {
            javaOffheapBytes = IOUtils.fromHumanReadableToByte(javaOffheap);
            // Ensure the provided javaOffheap is in JAVA_STYLE
            javaOffheap = IOUtils.javaStyleByteCount(javaOffheapBytes);
        }

        if (StringUtils.isEmpty(javaHeap)) {
            // TODO: Some of this magic should go to the JobManager
            long bytes;
            switch (job.getType()) {
                case WORKFLOW:
                case CUSTOM_TOOL:
                case VARIANT_WALKER:
                    // Because these type of jobs launch a separate docker instance, we should be more restrictive with the Java heap
                    bytes = IOUtils.fromHumanReadableToByte("2000Mi");
                    break;
                case NATIVE_TOOL:
                default:
                    String amount = Long.toString(memoryBytes);
                    bytes = IOUtils.fromHumanReadableToByte(amount);
                    bytes -= javaOffheapBytes;
                    bytes -= overHeadBytes;
                    break;
            }
            javaHeap = IOUtils.javaStyleByteCount(bytes);
        } else {
            // Ensure the provided javaHeap is in JAVA_STYLE
            long heapBytes = IOUtils.fromHumanReadableToByte(javaHeap);
            javaHeap = IOUtils.javaStyleByteCount(heapBytes);
        }

        envVars.add(new EnvVar("JAVA_OFF_HEAP", javaOffheap, null));
        if (javaHeap != null) {
            envVars.add(new EnvVar("JAVA_HEAP", javaHeap, null));
        }
        return envVars;
    }

    private boolean shouldAddDockerDaemon(String queue) {
//        return queue != null && queue.toLowerCase().contains("docker");
        return true;
    }

    /**
     * Build a valid K8S job name.
     * <p>
     * DNS-1123 label must consist of lower case alphanumeric characters or '-', and must start and
     * end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation
     * is '[a-z0-9]([-a-z0-9]*[a-z0-9])?'
     * <p>
     * Max length = 63
     * <p>
     * DNS-1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must
     * start and end with an alphanumeric character (e.g. 'example.com', regex used for validation
     * is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')
     *
     * @param jobIdInput job Is
     * @return valid name
     * @link https://github.com/kubernetes/kubernetes/blob/c560907/staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go#L135
     */
    protected static String buildJobName(String jobIdInput) {
        String jobId = jobIdInput.replace("_", "-");
        int[] invalidChars = jobId
                .chars()
                .filter(c -> c != '-' && !StringUtils.isAlphanumeric(String.valueOf((char) c)))
                .toArray();
        for (int invalidChar : invalidChars) {
            jobId = jobId.replace(((char) invalidChar), '-');
        }
        jobId = jobId.toLowerCase();
        boolean jobIdWithInvalidChars = !jobIdInput.equals(jobId);

        String jobName = ("opencga-job-" + jobId);
        if (jobName.length() > 63 || jobIdWithInvalidChars && jobName.length() > (63 - 8)) {
            // Job Id too large. Shrink it!
            // NOTE: This shrinking MUST be predictable!
            jobName = jobName.substring(0, 27)
                    + "-" + DigestUtils.md5Hex(jobIdInput).substring(0, 6).toLowerCase() + "-"
                    + jobName.substring(jobName.length() - 27);
        } else if (jobIdWithInvalidChars) {
            jobName += "-" + DigestUtils.md5Hex(jobIdInput).substring(0, 6).toLowerCase();
        }
        return jobName;
    }

    @Override
    public String getStatus(String jobId) {
        String k8sJobName = buildJobName(jobId);
        String status = jobStatusCache.compute(k8sJobName, (k, v) -> {
            if (v == null) {
                logger.warn("Missing job " + k8sJobName + " in cache. Fetch JOB info");
                return Pair.of(Instant.now(), getStatusForce(k));
            } else if (v.getKey().until(Instant.now(), ChronoUnit.MINUTES) > 10) {
                String newStatus = getStatusForce(k);
                String oldStatus = v.getValue();
                if (!oldStatus.equals(newStatus)) {
                    logger.warn("Update job " + k8sJobName + " from status cache. Change from " + oldStatus + " to " + newStatus);
                } else {
                    logger.debug("Update job " + k8sJobName + " from status cache. Status unchanged");
                }
                return Pair.of(Instant.now(), newStatus);
            }
            return v;
        }).getValue();
        logger.debug("Get status from job " + k8sJobName + ". Cache size: " + jobStatusCache.size() + " . Status: " + status);
        return status;
    }

    @Override
    public boolean stop(String jobId) throws Exception {
        return false;
    }

    @Override
    public boolean resume(String jobId) throws Exception {
        return false;
    }

    @Override
    public boolean kill(String jobId) throws Exception {
        String k8sJobName = buildJobName(jobId);

        switch (getStatus(jobId)) {
            case Enums.ExecutionStatus.DONE:
            case Enums.ExecutionStatus.ERROR:
            case Enums.ExecutionStatus.ABORTED: {
                return jobPodExists(k8sJobName);
            }
            case Enums.ExecutionStatus.UNKNOWN: {
                logger.warn("K8s Job '" + k8sJobName + "' not found!");
                return jobPodExists(k8sJobName);
            }
            case Enums.ExecutionStatus.QUEUED:
            case Enums.ExecutionStatus.PENDING:
            case Enums.ExecutionStatus.RUNNING: {
                deleteJobIfAny(k8sJobName);
                return jobPodExists(k8sJobName);
            }
            default:
                return false;
        }
    }

    private void deleteJobIfAny(String k8sJobName) {
        Job k8Job = getKubernetesClient()
                .batch()
                .v1()
                .jobs()
                .inNamespace(namespace)
                .withName(k8sJobName)
                .get();

        if (k8Job != null) {
            logger.info("Deleting kubernetes job '" + k8Job.getMetadata().getName() + "'");
            getKubernetesClient()
                    .batch()
                    .v1()
                    .jobs()
                    .inNamespace(namespace)
                    .withName(k8sJobName)
                    .withGracePeriod(terminationGracePeriodSeconds)
                    .delete();
        }
    }

    private boolean jobPodExists(String k8sJobName) {
        return getJobPod(k8sJobName) == null;
    }

    @Override
    public boolean isExecutorAlive() {
        return false;
    }

    /**
     * We do it this way to avoid writing the session id in the command line (avoid display/monitor/logs) attribute of Job.
     *
     * @param commandLine Basic command line
     * @param stdout      File where the standard output will be redirected
     * @param stderr      File where the standard error will be redirected
     * @return The complete command line
     */
    @Override
    public String getCommandLine(String commandLine, Path stdout, Path stderr) {
        // Do "exec" the main command to keep the PID 1 on the main process to allow grace kills.
        commandLine = "exec " + commandLine;
        // https://stackoverflow.com/questions/692000/how-do-i-write-standard-error-to-a-file-while-using-tee-with-a-pipe
        if (stderr != null) {
            if (logToStdout) {
                commandLine = commandLine + " 2> >( tee -a \"" + stderr.toAbsolutePath() + "\" >&2 )";
            } else {
                commandLine = commandLine + " 2>> " + stderr.toAbsolutePath();
            }
        }
        if (stdout != null) {
            if (logToStdout) {
                commandLine = commandLine + " > >( tee -a \"" + stdout.toAbsolutePath() + "\")";
            } else {
                commandLine = commandLine + " >> " + stdout.toAbsolutePath();
            }
        }

        // Add trap to capture TERM signal and kill the main process
        String trapTerm = "trap '"
                + "echo \"Job terminated! Run time : ${SECONDS}s\" ;\n"
                + "touch INTERRUPTED ;\n"
                + "if [ -s PID ] && ps -p $(cat PID) > /dev/null; then\n"
                + "  kill -15 $(cat PID) ;\n"
                + "fi' TERM ;";

        // Launch the main process in background.
        String mainProcess = commandLine + " &";

        // Wait for the main process to finish and capture its PID.
        // Active wait instead of `wait` to allow trap to kill -15 the job
        // We will use this PID to kill the main process if the job is interrupted.
        String wait = "PID=$! ; \n"
                + "echo $PID > PID ; \n"
                + "while ps -p \"$PID\" >/dev/null; do \n"
                + "    sleep 1 ; \n"
                + "done \n";

        // Create a file to indicate that the dind sidecar container should finish
        String dindDone = "touch '" + DIND_DONE_FILE + "' \n";

        // If the job was interrupted, exit with error
        String exitIfInterrupted = "if [ -f INTERRUPTED ]; then\n"
                + "  exit 1\n"
                + "fi \n";

        // Capture error code and forward it
        String captureErrorCode = "wait $PID\n"
                + "ERRCODE=$? \n"
                + "exit $ERRCODE";

        return trapTerm + " "
                + mainProcess + " "
                + wait + " "
                + dindDone + " "
                + exitIfInterrupted + " "
                + captureErrorCode;
    }

    private String getStatusForce(String k8sJobName) {
        Job k8Job = getKubernetesClient()
                .batch()
                .jobs()
                .inNamespace(namespace)
                .withName(k8sJobName)
                .get();

        if (k8Job == null) {
            logger.warn("Job '" + k8sJobName + "' not found!");
            // Job has been deleted. manually?
            return Enums.ExecutionStatus.ABORTED;
        }

        String statusFromK8sJob = getStatusFromK8sJob(k8Job, k8sJobName);
        if (statusFromK8sJob.equalsIgnoreCase(Enums.ExecutionStatus.UNKNOWN)
                || statusFromK8sJob.equalsIgnoreCase(Enums.ExecutionStatus.QUEUED)) {
            logger.warn("Job status " + statusFromK8sJob + " . Fetch POD info");
            Pod pod = getJobPod(k8sJobName);
            if (pod != null) {
                return getStatusFromPod(pod);
            }
        }
        return statusFromK8sJob;
    }

    private Pod getJobPod(String k8sJobName) {
        List<Pod> pods = getKubernetesClient().pods().withLabel(JOB_NAME, k8sJobName).list(1, null).getItems();
        if (pods.isEmpty()) {
            return null;
        } else {
            return pods.get(0);
        }
    }

    private String getJobName(Pod pod) {
        if (pod.getMetadata() == null
                || pod.getMetadata().getLabels() == null
                || pod.getStatus() == null
                || pod.getStatus().getPhase() == null) {
            return null;
        }
        return pod.getMetadata().getLabels().get(JOB_NAME);
    }

    private String getStatusFromPod(Pod pod) {
        final String status;
        String phase = pod.getStatus().getPhase();
        switch (phase.toLowerCase()) {
            case "succeeded":
                status = Enums.ExecutionStatus.DONE;
                break;
            case "pending":
                status = Enums.ExecutionStatus.QUEUED;
                break;
            case "error":
            case "failed":
                status = Enums.ExecutionStatus.ERROR;
                break;
            case "running":
                status = Enums.ExecutionStatus.RUNNING;
                break;
            default:
                logger.warn("Unknown POD phase " + phase);
                status = Enums.ExecutionStatus.UNKNOWN;
                break;
        }
        return status;
    }

    private String getStatusFromK8sJob(Job k8Job, String k8sJobName) {
        String status;
        if (k8Job == null || k8Job.getStatus() == null) {
            logger.debug("k8Job '{}' status = null", k8sJobName);
            status = Enums.ExecutionStatus.UNKNOWN;
        } else if (k8Job.getStatus().getSucceeded() != null && k8Job.getStatus().getSucceeded() > 0) {
            status = Enums.ExecutionStatus.DONE;
        } else if (k8Job.getStatus().getFailed() != null && k8Job.getStatus().getFailed() > 0) {
            status = Enums.ExecutionStatus.ERROR;
        } else if (k8Job.getStatus().getActive() != null && k8Job.getStatus().getActive() > 0) {
//            status = Enums.ExecutionStatus.RUNNING;
            status = Enums.ExecutionStatus.QUEUED;
        } else {
            status = Enums.ExecutionStatus.UNKNOWN;
        }
        logger.debug("k8Job '{}}' status = '{}'", k8sJobName, status);
        return status;
    }

    private Map<String, String> getMap(ObjectMap objectMap, String key) {
        if (objectMap != null && objectMap.containsKey(key)) {
            Map<String, String> map = new HashMap<>();
            ((Map<String, Object>) objectMap.get(key)).forEach((k, v) -> map.put(k, v.toString()));
            return map;
        } else {
            return Collections.emptyMap();
        }
    }

    private <T> List<T> buildObjects(List<Object> list, Class<T> clazz) {
        List<T> ts = new ArrayList<>();
        if (list == null) {
            return ts;
        }
        ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
        for (Object o : list) {
            ts.add(mapper.convertValue(o, clazz));
        }
        return ts;
    }

    private <T> T buildObject(Object o, Class<T> clazz) {
        if (o == null) {
            return null;
        }
        ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
        return mapper.convertValue(o, clazz);
    }

    private List<VolumeMount> buildVolumeMounts(List<Object> list) {
        return buildObjects(list, VolumeMount.class);
    }

    private List<LocalObjectReference> buildLocalObjectReference(Object object) {
        LocalObjectReference reference = buildObject(object, LocalObjectReference.class);
        return reference == null ? Collections.emptyList() : Collections.singletonList(reference);
    }

    private List<Volume> buildVolumes(List<Object> list) {
        return buildObjects(list, Volume.class);
    }

    private List<Toleration> buildTolelrations(List<Object> list) {
        return buildObjects(list, Toleration.class);
    }

    private KubernetesClient getKubernetesClient() {
        return kubernetesClient;
    }

}
