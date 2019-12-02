package org.opencb.opencga.catalog.monitor.executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.common.Enums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


public class K8SExecutor implements BatchExecutor {

    public static final String K8S_MASTER_NODE = "k8s.masterNode";
    public static final String K8S_IMAGE_NAME = "k8s.imageName";
    public static final String K8S_CPU = "k8s.cpu";
    public static final String K8S_MEMORY = "k8s.memory";
    public static final String K8S_NAMESPACE = "k8s.namespace";
    public static final String K8S_VOLUMES_MOUNT = "k8S.volumesMount";

    public static final String K8S_KIND = "Job";

    private String k8sClusterMaster;
    private String namespace;
    private String imageName;
    private String cpu;
    private String memory;
    private List<VolumeMount> volumeMounts;
    private Config k8sConfig;
    private KubernetesClient kubernetesClient;
    private static Logger logger = LoggerFactory.getLogger(K8SExecutor.class);

    public K8SExecutor(Execution execution) {
        this.k8sClusterMaster = execution.getOptions().getString(K8S_MASTER_NODE);
        this.namespace = execution.getOptions().getString(K8S_NAMESPACE);
        this.imageName = execution.getOptions().getString(K8S_IMAGE_NAME);
        this.cpu = execution.getOptions().getString(K8S_CPU);
        this.memory = execution.getOptions().getString(K8S_MEMORY);
        this.volumeMounts = buildVolumeMounts(execution.getOptions().getList(K8S_VOLUMES_MOUNT));
        this.k8sConfig = new ConfigBuilder().withMasterUrl(k8sClusterMaster).build();
        this.kubernetesClient = new DefaultKubernetesClient(k8sConfig).inNamespace(namespace);
    }

    @Override
    public void execute(String jobId, String commandLine, Path stdout, Path stderr) throws Exception {
        HashMap<String, Quantity> requests = new HashMap<>();
        requests.put("cpu", new Quantity(this.cpu));
        requests.put("memory", new Quantity(this.memory));

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
                            .withNodeSelector(Collections.singletonMap("node", "worker"))
                            .withRestartPolicy("Never")
                            .withVolumes(
                                    new VolumeBuilder()
                                            .withName("conf")
                                            .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                                    .withDefaultMode(0555) // r-x r-x r-x
                                                    .withName("conf").build()).build(),
                                    new VolumeBuilder()
                                            .withName("confhadoop")
                                            .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                                    .withDefaultMode(0555) // r-x r-x r-x
                                                    .withName("confhadoop").build()).build(),
                                    new VolumeBuilder()
                                            .withName("opencga-shared")
                                            .withNewPersistentVolumeClaim("opencga-storage-claim", false).build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

            getKubernetesClient().batch().jobs().inNamespace(namespace).create(k8sJob);
    }

    /**
     * Build a valid K8S job name.
     *
     * DNS-1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must
     * start and end with an alphanumeric character (e.g. 'example.com', regex used for validation
     * is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')
     * @param jobId job Is
     * @return valid name
     */
    protected static String buildJobName(String jobId) {
        jobId = jobId.replace("_", "-");
        int[] invalidChars = jobId
                .chars()
                .filter(c -> c != '-' && c != '.' && !StringUtils.isAlphanumeric(String.valueOf((char) c)))
                .toArray();
        for (int invalidChar : invalidChars) {
            jobId = jobId.replace(((char) invalidChar), '.');
        }
        return ("opencga-job-" + jobId).toLowerCase();
    }

    @Override
    public String getStatus(String jobId) {

        String k8sJobName = buildJobName(jobId);
        ScalableResource<io.fabric8.kubernetes.api.model.batch.Job, DoneableJob> resource =
                getKubernetesClient().batch().jobs().inNamespace(namespace).withName(k8sJobName);

        io.fabric8.kubernetes.api.model.batch.Job k8Job = resource.get();

        if (k8Job == null || k8Job.getStatus() == null) {
            logger.debug("k8Job '" + k8sJobName + "' status = " + Enums.ExecutionStatus.UNKNOWN + ". Missing k8sJob");
            return Enums.ExecutionStatus.UNKNOWN;
        } else if (k8Job.getStatus().getActive() != null && k8Job.getStatus().getActive() > 0) {
            logger.debug("k8Job '" + k8sJobName + "' status = " + Enums.ExecutionStatus.RUNNING);
            return Enums.ExecutionStatus.RUNNING;
        } else if (k8Job.getStatus().getSucceeded() != null && k8Job.getStatus().getSucceeded() > 0) {
            logger.debug("k8Job '" + k8sJobName + "' status = " + Enums.ExecutionStatus.DONE);
            return Enums.ExecutionStatus.DONE;
        } else if (k8Job.getStatus().getFailed() != null && k8Job.getStatus().getFailed() > 0) {
            logger.debug("k8Job '" + k8sJobName + "' status = " + Enums.ExecutionStatus.ERROR);
            return Enums.ExecutionStatus.ERROR;
        }
        logger.debug("k8Job '" + k8sJobName + "' status = " + Enums.ExecutionStatus.UNKNOWN);
        return Enums.ExecutionStatus.UNKNOWN;
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
