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
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.common.Enums;
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
    public static final String K8S_REQUESTS = "k8s.requests";
    public static final String K8S_LIMITS = "k8s.limits";
    public static final String K8S_JAVA_HEAP = "k8s.javaHeap";
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
    private final ResourceRequirements resources;
    private final List<EnvVar> envVars;
    private final Config k8sConfig;
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
    private long terminationGracePeriodSeconds;

    public K8SExecutor(Configuration configuration) {
        Execution execution = configuration.getAnalysis().getExecution();
        String k8sClusterMaster = execution.getOptions().getString(K8S_MASTER_NODE);
        this.namespace = execution.getOptions().getString(K8S_NAMESPACE);
        this.imageName = execution.getOptions().getString(K8S_IMAGE_NAME);
        this.volumeMounts = buildVolumeMounts(execution.getOptions().getList(K8S_VOLUME_MOUNTS));
        this.volumes = buildVolumes(execution.getOptions().getList(K8S_VOLUMES));
        this.tolerations = buildTolelrations(execution.getOptions().getList(K8S_TOLERATIONS));
        this.k8sConfig = new ConfigBuilder()
                .withMasterUrl(k8sClusterMaster)
                // Connection timeout in ms (0 for no timeout)
                .withConnectionTimeout(execution.getOptions().getInt(K8S_CLIENT_TIMEOUT, DEFAULT_TIMEOUT))
                // Read timeout in ms
                .withRequestTimeout(execution.getOptions().getInt(K8S_CLIENT_TIMEOUT, 30000))
                .build();
        this.kubernetesClient = new DefaultKubernetesClient(k8sConfig).inNamespace(namespace);
        this.imagePullPolicy = execution.getOptions().getString(K8S_IMAGE_PULL_POLICY, "IfNotPresent");
        this.imagePullSecrets = buildLocalObjectReference(execution.getOptions().get(K8S_IMAGE_PULL_SECRETS));
        this.ttlSecondsAfterFinished = execution.getOptions().getInt(K8S_TTL_SECONDS_AFTER_FINISHED, 3600);
        this.terminationGracePeriodSeconds = execution.getOptions().getInt(K8S_TERMINATION_GRACE_PERIOD_SECONDS, 5 * 60);
        this.logToStdout = execution.getOptions().getBoolean(K8S_LOG_TO_STDOUT, true);
        nodeSelector = getMap(execution.getOptions(), K8S_NODE_SELECTOR);
        if (execution.getOptions().containsKey(K8S_SECURITY_CONTEXT)) {
            securityContext = buildObject(execution.getOptions().get(K8S_SECURITY_CONTEXT), SecurityContext.class);
        } else {
            securityContext = new SecurityContextBuilder()
                    .withRunAsUser(1001L)
                    .withRunAsNonRoot(true)
                    .withReadOnlyRootFilesystem(false)
                    .build();
        }
        if (execution.getOptions().containsKey(K8S_POD_SECURITY_CONTEXT)) {
            podSecurityContext = buildObject(execution.getOptions().get(K8S_POD_SECURITY_CONTEXT), PodSecurityContext.class);
        } else {
            podSecurityContext = new PodSecurityContextBuilder()
                    .withRunAsNonRoot(true)
                    .build();
        }

        HashMap<String, Quantity> requests = new HashMap<>();
        for (Map.Entry<String, String> entry : getMap(execution.getOptions(), K8S_REQUESTS).entrySet()) {
            requests.put(entry.getKey(), new Quantity(entry.getValue()));
        }
        HashMap<String, Quantity> limits = new HashMap<>();
        for (Map.Entry<String, String> entry : getMap(execution.getOptions(), K8S_LIMITS).entrySet()) {
            limits.put(entry.getKey(), new Quantity(entry.getValue()));
        }
        resources = new ResourceRequirementsBuilder()
                .withLimits(limits)
                .withRequests(requests)
                .build();
        envVars = new ArrayList<>();
        envVars.add(DOCKER_HOST);

        String javaHeap = execution.getOptions().getString(K8S_JAVA_HEAP);
        if (StringUtils.isEmpty(javaHeap) && requests.containsKey("memory")) {
            Quantity memory = requests.get("memory");
            String amount = memory.getAmount();
            long bytes = IOUtils.fromHumanReadableToByte(amount);
            bytes -= IOUtils.fromHumanReadableToByte("300Mi");
            javaHeap = Long.toString(bytes);
        }
        if (javaHeap != null) {
            envVars.add(new EnvVar("JAVA_HEAP", javaHeap, null));
        }
        Map<String, String> map = getMap(execution.getOptions(), K8S_ENVS);
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

        String dindImageName = execution.getOptions().getString(K8S_DIND_IMAGE_NAME, "docker:dind-rootless");
        boolean rootless;
        if (execution.getOptions().containsKey(K8S_DIND_ROOTLESS)) {
            rootless = execution.getOptions().getBoolean(K8S_DIND_ROOTLESS);
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
            dindSecurityContext = new SecurityContextBuilder().withPrivileged(true).build();
        }
        dockerDaemonSidecar = new ContainerBuilder()
                .withName("dind-daemon")
                .withImage(dindImageName)
                .withSecurityContext(dindSecurityContext)
                .withEnv(new EnvVar("DOCKER_TLS_CERTDIR", "", null))
//                .withResources(resources) // TODO: Should we add resources here?
                .withCommand("/bin/sh", "-c")
                .addToArgs("dockerd-entrypoint.sh & "
                        // Add trap to capture TERM signal and finish main process
                        + "trap '"
                        + "echo \"Container terminated! ;\n"
                        + "touch " + DIND_DONE_FILE + " ' TERM ;"
                        + "while ! test -f " + DIND_DONE_FILE + "; do sleep 5; done; exit 0")
                .addToVolumeMounts(DOCKER_GRAPH_VOLUMEMOUNT)
                .addToVolumeMounts(TMP_POD_VOLUMEMOUNT)
                .addAllToVolumeMounts(volumeMounts)
                .build();

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
        ResourceRequirements resources = getResources(job);
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
                                .withSpec(new PodSpecBuilder()
                                        .withTerminationGracePeriodSeconds(terminationGracePeriodSeconds)
                                        .withImagePullSecrets(imagePullSecrets)
                                        .addToContainers(new ContainerBuilder()
                                                .withName("opencga")
                                                .withImage(imageName)
                                                .withImagePullPolicy(imagePullPolicy)
                                                .withResources(resources)
                                                .addAllToEnv(envVars)
                                                .withCommand("/bin/bash", "-c")
                                                .withArgs(getCommandLine(commandLine, stdout, stderr))
                                                .withVolumeMounts(volumeMounts)
                                                .addToVolumeMounts(TMP_POD_VOLUMEMOUNT)
                                                .withSecurityContext(securityContext)
                                                .build())
                                        .withNodeSelector(nodeSelector)
                                        .withTolerations(tolerations)
                                        .withRestartPolicy("Never")
                                        .addAllToVolumes(volumes)
                                        .addToVolumes(DOCKER_GRAPH_STORAGE_VOLUME)
                                        .addToVolumes(TMP_POD_VOLUME)
                                        .withSecurityContext(podSecurityContext)
                                        .build())
                                .build())
                        .build()
                ).build();

        if (shouldAddDockerDaemon(queue)) {
            k8sJob.getSpec().getTemplate().getSpec().getContainers().add(dockerDaemonSidecar);
        }
        jobStatusCache.put(jobName, Pair.of(Instant.now(), Enums.ExecutionStatus.QUEUED));
        getKubernetesClient().batch().v1().jobs().inNamespace(namespace).resource(k8sJob).create();
    }

    private ResourceRequirements getResources(org.opencb.opencga.core.models.job.Job job) {
        if (job.getTool().getMinimumRequirements() != null) {
            ResourceRequirementsBuilder resources = new ResourceRequirementsBuilder(this.resources);
            if (StringUtils.isNotEmpty(job.getTool().getMinimumRequirements().getMemory())) {
                long memoryBytes = IOUtils.fromHumanReadableToByte(job.getTool().getMinimumRequirements().getMemory());
                Quantity memory = new Quantity(String.valueOf(memoryBytes));
                resources.addToRequests("memory", memory);
                resources.addToLimits("memory", memory);
            }
            if (StringUtils.isNotEmpty(job.getTool().getMinimumRequirements().getCpu())) {
                double cpuUnits = Double.parseDouble(job.getTool().getMinimumRequirements().getCpu());
                Quantity cpu = new Quantity(Double.toString(cpuUnits));
                resources.addToRequests("cpu", cpu);
                resources.addToLimits("cpu", cpu);
            }
            return resources.build();
        } else {
            return this.resources;
        }
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
        if (objectMap.containsKey(key)) {
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
