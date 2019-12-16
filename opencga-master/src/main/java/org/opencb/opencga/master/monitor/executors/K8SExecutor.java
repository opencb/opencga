package org.opencb.opencga.master.monitor.executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.common.Enums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class K8SExecutor implements BatchExecutor {

    public static final String K8S_MASTER_NODE = "k8s.masterNode";
    public static final String K8S_IMAGE_NAME = "k8s.imageName";
    public static final String K8S_CPU = "k8s.cpu";
    public static final String K8S_MEMORY = "k8s.memory";
    public static final String K8S_NAMESPACE = "k8s.namespace";
    public static final String K8S_VOLUMES_MOUNT = "k8s.volumesMount";
    public static final String K8S_NODE_SELECTOR = "k8s.nodeSelector";

    public static final String K8S_KIND = "Job";

    private final String k8sClusterMaster;
    private final String namespace;
    private final String imageName;
    private final List<VolumeMount> volumeMounts;
    private final List<Volume> volumes;
    private final Map<String, String> nodeSelector;
    private final Config k8sConfig;
    private final KubernetesClient kubernetesClient;
    private static Logger logger = LoggerFactory.getLogger(K8SExecutor.class);

    private final Map<String, String> jobStatusCache = new ConcurrentHashMap<>();
    private final Watch podsWatcher;
    private final Watch jobsWatcher;
    private final HashMap<String, Quantity> requests;

    public K8SExecutor(Execution execution) {
        this.k8sClusterMaster = execution.getOptions().getString(K8S_MASTER_NODE);
        this.namespace = execution.getOptions().getString(K8S_NAMESPACE);
        this.imageName = execution.getOptions().getString(K8S_IMAGE_NAME);
        List<Object> list = execution.getOptions().getList(K8S_VOLUMES_MOUNT);
        if (CollectionUtils.isEmpty(list)) {
            list = execution.getOptions().getList("k8S.volumesMount");
        }
        this.volumeMounts = buildVolumeMounts(list);
        this.volumes = buildVolumes(list);
        this.k8sConfig = new ConfigBuilder().withMasterUrl(k8sClusterMaster).build();
        this.kubernetesClient = new DefaultKubernetesClient(k8sConfig).inNamespace(namespace);

        String cpu = execution.getOptions().getString(K8S_CPU);
        String memory = execution.getOptions().getString(K8S_MEMORY);
        requests = new HashMap<>();
        requests.put("cpu", new Quantity(cpu));
        requests.put("memory", new Quantity(memory));

        if (execution.getOptions().containsKey(K8S_NODE_SELECTOR)) {
            this.nodeSelector = new HashMap<>();
            ((Map<String, Object>) execution.getOptions().get(K8S_NODE_SELECTOR)).forEach((k, v) -> nodeSelector.put(k, v.toString()));
        } else {
            this.nodeSelector = Collections.emptyMap();
        }

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
            }
        });
    }

    @Override
    public void execute(String jobId, String commandLine, Path stdout, Path stderr) throws Exception {
        String jobName = buildJobName(jobId);
        final io.fabric8.kubernetes.api.model.batch.Job k8sJob = new JobBuilder()
                .withApiVersion("batch/v1")
                .withKind(K8S_KIND)
                .withNewMetadata()
                    .withName(jobName)
                    .withLabels(Collections.singletonMap("opencga", "job"))
    //                        .withAnnotations(Collections.singletonMap("variantFileSize", Long.toString(job.getSize())))
                .endMetadata()
                .withNewSpec()
                    .withTtlSecondsAfterFinished(30)
                    .withBackoffLimit(0) // specify the number of retries before considering a Job as failed
                    .withNewTemplate()
                        .withNewSpec()
                            .addNewContainer()
                                .withName(jobName)
                                .withImage(this.imageName)
                                .withImagePullPolicy("Always")
                                .withResources(new ResourceRequirementsBuilder().withRequests(requests).build())
                                .withArgs("/bin/sh", "-c", getCommandLine(commandLine, stdout, stderr))
                                .withVolumeMounts(this.volumeMounts)
                            .endContainer()
                            .withNodeSelector(nodeSelector)
                            .withRestartPolicy("Never")
                            .withVolumes(volumes)
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

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

    private List<VolumeMount> buildVolumeMounts(List<Object> k8SVolumesMounts) {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        for (Object o : k8SVolumesMounts) {
            K8SVolumesMount k8SVolumesMount;
            try {
                ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
                k8SVolumesMount = mapper.readValue(mapper.writeValueAsString(o), K8SVolumesMount.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            volumeMounts.add(new VolumeMountBuilder()
                    .withName(k8SVolumesMount.name)
                    .withMountPath(k8SVolumesMount.mountPath)
                    .withReadOnly(k8SVolumesMount.readOnly)
                    .build());
        }
        return volumeMounts;
    }

    private List<Volume> buildVolumes(List<Object> k8SVolumesMounts) {
        List<Volume> volumes = new ArrayList<>();
        for (Object o : k8SVolumesMounts) {
            K8SVolumesMount k8SVolumesMount;
            try {
                ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
                k8SVolumesMount = mapper.readValue(mapper.writeValueAsString(o), K8SVolumesMount.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            VolumeBuilder vb = new VolumeBuilder()
                    .withName(k8SVolumesMount.name);
            if (k8SVolumesMount.name.startsWith("conf")) {
                vb.withConfigMap(new ConfigMapVolumeSourceBuilder()
                        .withDefaultMode(0555) // r-x r-x r-x
                        .withName(k8SVolumesMount.name).build());
            } else {
                vb.withNewPersistentVolumeClaim(k8SVolumesMount.name, false);
            }
            volumes.add(vb.build());
        }
        return volumes;
    }

    private KubernetesClient getKubernetesClient() {
        return kubernetesClient == null ? new DefaultKubernetesClient(k8sConfig).inNamespace(namespace) : this.kubernetesClient;
    }

    private static class K8SVolumesMount {
        private String name;
        private String mountPath;
        private boolean readOnly;

        public String getName() {
            return name;
        }

        public K8SVolumesMount setName(String name) {
            this.name = name;
            return this;
        }

        public String getMountPath() {
            return mountPath;
        }

        public K8SVolumesMount setMountPath(String mountPath) {
            this.mountPath = mountPath;
            return this;
        }

        public boolean isReadOnly() {
            return readOnly;
        }

        public K8SVolumesMount setReadOnly(boolean readOnly) {
            this.readOnly = readOnly;
            return this;
        }
    }
}
