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
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.JobSpecBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.common.Enums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class K8SExecutor implements BatchExecutor {

    public static final String K8S_MASTER_NODE = "k8s.masterUrl";
    public static final String K8S_IMAGE_NAME = "k8s.imageName";
    public static final String K8S_REQUESTS = "k8s.requests";
    public static final String K8S_LIMITS = "k8s.limits";
    public static final String K8S_NAMESPACE = "k8s.namespace";
    public static final String K8S_VOLUME_MOUNTS = "k8s.volumeMounts";
    public static final String K8S_VOLUMES = "k8s.volumes";
    public static final String K8S_NODE_SELECTOR = "k8s.nodeSelector";
    public static final String K8S_TOLERATIONS = "k8s.tolerations";

    private final String k8sClusterMaster;
    private final String namespace;
    private final String imageName;
    private final List<VolumeMount> volumeMounts;
    private final List<Volume> volumes;
    private final Map<String, String> nodeSelector;
    private final List<Toleration> tolerations;
    private final ResourceRequirements resources;
    private final Config k8sConfig;
    private final KubernetesClient kubernetesClient;
    private static Logger logger = LoggerFactory.getLogger(K8SExecutor.class);

    private final Map<String, String> jobStatusCache = new ConcurrentHashMap<>();
    private final Watch podsWatcher;
    private final Watch jobsWatcher;

    public K8SExecutor(Execution execution) {
        this.k8sClusterMaster = execution.getOptions().getString(K8S_MASTER_NODE);
        this.namespace = execution.getOptions().getString(K8S_NAMESPACE);
        this.imageName = execution.getOptions().getString(K8S_IMAGE_NAME);
        this.volumeMounts = buildVolumeMounts(execution.getOptions().getList(K8S_VOLUME_MOUNTS));
        this.volumes = buildVolumes(execution.getOptions().getList(K8S_VOLUMES));
        this.tolerations = buildTolelrations(execution.getOptions().getList(K8S_TOLERATIONS));
        this.k8sConfig = new ConfigBuilder().withMasterUrl(k8sClusterMaster).build();
        this.kubernetesClient = new DefaultKubernetesClient(k8sConfig).inNamespace(namespace);

        nodeSelector = getMap(execution, K8S_NODE_SELECTOR);

        HashMap<String, Quantity> requests = new HashMap<>();
        for (Map.Entry<String, String> entry : getMap(execution, K8S_REQUESTS).entrySet()) {
            requests.put(entry.getKey(), new Quantity(entry.getValue()));
        }
        HashMap<String, Quantity> limits = new HashMap<>();
        for (Map.Entry<String, String> entry : getMap(execution, K8S_LIMITS).entrySet()) {
            limits.put(entry.getKey(), new Quantity(entry.getValue()));
        }
        resources = new ResourceRequirementsBuilder()
                .withLimits(limits)
                .withRequests(requests)
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
                    jobStatusCache.put(k8sJobName, status);
                }
            }

            @Override
            public void onClose(KubernetesClientException e) {
                if (e != null) {
                    logger.error("Catch exception at jobs watcher", e);
                    throw e;
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
                    jobStatusCache.put(k8jobName, status);
                }
            }

            @Override
            public void onClose(KubernetesClientException e) {
                if (e != null) {
                    logger.error("Catch exception at pods watcher", e);
                    throw e;
                }
            }
        });
    }

    @Override
    public void execute(String jobId, String commandLine, Path stdout, Path stderr) throws Exception {
        String jobName = buildJobName(jobId);
        final io.fabric8.kubernetes.api.model.batch.Job k8sJob = new JobBuilder()
                .withApiVersion("batch/v1")
                .withKind("Job")
                .withMetadata(new ObjectMetaBuilder()
                        .withName(jobName)
                        .withLabels(Collections.singletonMap("opencga", "job"))
                        .build())
                .withSpec(new JobSpecBuilder()
                        .withTtlSecondsAfterFinished(30)
                        .withBackoffLimit(0) // specify the number of retries before considering a Job as failed
                        .withTemplate(new PodTemplateSpecBuilder()
                                .withSpec(new PodSpecBuilder()
                                        .withContainers(new ContainerBuilder()
                                                .withName(jobName)
                                                .withImage(imageName)
                                                .withImagePullPolicy("Always")
                                                .withResources(resources)
                                                .withCommand("/bin/sh")
                                                .withArgs("-c", getCommandLine(commandLine, stdout, stderr))
                                                .withVolumeMounts(volumeMounts)
                                                .build())
                                        .withNodeSelector(nodeSelector)
                                        .withTolerations(tolerations)
                                        .withRestartPolicy("Never")
                                        .withVolumes(volumes)
                                        .build())
                                .build())
                        .build()
                ).build();

        jobStatusCache.put(jobName, Enums.ExecutionStatus.QUEUED);
        getKubernetesClient().batch().jobs().inNamespace(namespace).create(k8sJob);
    }

    /**
     * Build a valid K8S job name.
     *
     * DNS-1123 label must consist of lower case alphanumeric characters or '-', and must start and
     * end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation
     * is '[a-z0-9]([-a-z0-9]*[a-z0-9])?'
     *
     * Max length = 63
     *
     * DNS-1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must
     * start and end with an alphanumeric character (e.g. 'example.com', regex used for validation
     * is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')
     * @param jobId job Is
     * @link https://github.com/kubernetes/kubernetes/blob/c560907/staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go#L135
     * @return valid name
     */
    protected static String buildJobName(String jobId) {
        jobId = jobId.replace("_", "-");
        int[] invalidChars = jobId
                .chars()
                .filter(c -> c != '-' && !StringUtils.isAlphanumeric(String.valueOf((char) c)))
                .toArray();
        for (int invalidChar : invalidChars) {
            jobId = jobId.replace(((char) invalidChar), '-');
        }
        String jobName = ("opencga-job-" + jobId).toLowerCase();
        if (jobName.length() > 63) {
            jobName = jobName.substring(0, 30) + "--" + jobName.substring(jobName.length() - 30);
        }
        return jobName;
    }

    @Override
    public String getStatus(String jobId) {
        String k8sJobName = buildJobName(jobId);
        String status = jobStatusCache.compute(k8sJobName, (k, v) -> v == null ? getStatusForce(k) : v);
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
        return false;
    }

    @Override
    public boolean isExecutorAlive() {
        return false;
    }

    private String getStatusForce(String k8sJobName) {
        logger.warn("Missing job " + k8sJobName + " in cache. Fetch JOB info");
        Job k8Job = getKubernetesClient()
                .batch()
                .jobs()
                .inNamespace(namespace)
                .withName(k8sJobName)
                .get();

        if (k8Job == null) {
            logger.warn("Job " + k8sJobName + " not found!");
            // Job has been deleted. manually?
            return Enums.ExecutionStatus.ABORTED;
        }

        String statusFromK8sJob = getStatusFromK8sJob(k8Job, k8sJobName);
        if (statusFromK8sJob.equalsIgnoreCase(Enums.ExecutionStatus.UNKNOWN)
                || statusFromK8sJob.equalsIgnoreCase(Enums.ExecutionStatus.QUEUED)) {
            logger.warn("Job status " + statusFromK8sJob + " . Fetch POD info");
            List<Pod> pods = getKubernetesClient().pods().withLabel("job-name", k8sJobName).list(1, null).getItems();
            if (!pods.isEmpty()) {
                Pod pod = pods.get(0);
                return getStatusFromPod(pod);
            }
        }
        return statusFromK8sJob;
    }

    private String getJobName(Pod pod) {
        if (pod.getMetadata() == null
                || pod.getMetadata().getLabels() == null
                || pod.getStatus() == null
                || pod.getStatus().getPhase() == null) {
            return null;
        }
        return pod.getMetadata().getLabels().get("job-name");
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
        }  else {
            status = Enums.ExecutionStatus.UNKNOWN;
        }
        logger.debug("k8Job '{}}' status = '{}'", k8sJobName, status);
        return status;
    }

    private Map<String, String> getMap(Execution execution, String key) {
        if (execution.getOptions().containsKey(key)) {
            Map<String, String> map = new HashMap<>();
            ((Map<String, Object>) execution.getOptions().get(key)).forEach((k, v) -> map.put(k, v.toString()));
            return map;
        } else {
            return Collections.emptyMap();
        }
    }

    private List<VolumeMount> buildVolumeMounts(List<Object> list) {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        if (list == null) {
            return volumeMounts;
        }
        for (Object o : list) {
            ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
            volumeMounts.add(mapper.convertValue(o, VolumeMount.class));
        }
        return volumeMounts;
    }

    private List<Volume> buildVolumes(List<Object> list) {
        List<Volume> volumes = new ArrayList<>();
        if (list == null) {
            return volumes;
        }
        for (Object o : list) {
            ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
            volumes.add(mapper.convertValue(o, Volume.class));
        }
        return volumes;
    }

    private List<Toleration> buildTolelrations(List<Object> list) {
        List<Toleration> tolerations = new ArrayList<>();
        if (list == null) {
            return tolerations;
        }
        for (Object o : list) {
            ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
            tolerations.add(mapper.convertValue(o, Toleration.class));
        }
        return tolerations;
    }

    private KubernetesClient getKubernetesClient() {
        return kubernetesClient == null ? new DefaultKubernetesClient(k8sConfig).inNamespace(namespace) : this.kubernetesClient;
    }

}
