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
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.Job;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.*;


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
    public void execute(String jobId, String commandLine, Path stdout, Path stderr, String token) throws Exception {
        HashMap<String, Quantity> requests = new HashMap<>();
        requests.put("cpu", new Quantity(this.cpu));
        requests.put("memory", new Quantity(this.memory));

        String jobName = "opencga-job-" + jobId.replace("_", "-");
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
                    .withNewTemplate()
                    .withNewSpec()
                    .addNewContainer()
                    .withName(jobName)
                    .withImage(this.imageName)
                    .withImagePullPolicy("Always")
                    .withResources(new ResourceRequirementsBuilder().withRequests(requests).build())
                    .withArgs("/bin/sh", "-c", commandLine)
                    .withVolumeMounts(this.volumeMounts)
                    .endContainer()

                .withNodeSelector(Collections.singletonMap("node", "worker"))
                    .withRestartPolicy("Never")
                    .withVolumes(new VolumeBuilder().withName("conf").withConfigMap(new ConfigMapVolumeSourceBuilder()
                                    .withName("conf").build()).build(),
                            new VolumeBuilder().withName("opencga-shared")
                                    .withNewPersistentVolumeClaim("opencga-storage-claim", false).build())
                    .endSpec()
                    .endTemplate()
                    .endSpec()
                    .build();

            getKubernetesClient().batch().jobs().inNamespace(namespace).create(k8sJob);
    }

    @Override
    public String getStatus(Job job) {

            ScalableResource<io.fabric8.kubernetes.api.model.batch.Job, DoneableJob> k8Job =
                    getKubernetesClient().batch().jobs().inNamespace(namespace).withName("opencga-job-" + job.getId().replace("_", "-"));

            if (k8Job.get().getStatus().getActive() > 0) {
                return Job.JobStatus.RUNNING;
            } else if (k8Job.get().getStatus().getSucceeded() > 0) {
                return Job.JobStatus.DONE;
            } else if (k8Job.get().getStatus().getFailed() > 0) {
                return Job.JobStatus.ERROR;
            }
            return Job.JobStatus.UNKNOWN;
    }

    @Override
    public boolean stop(Job job) throws Exception {
        return false;
    }

    @Override
    public boolean resume(Job job) throws Exception {
        return false;
    }

    @Override
    public boolean kill(Job job) throws Exception {
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
            volumeMounts.add(new VolumeMountBuilder().withName(k8SVolumesMount.getName())
                    .withMountPath(k8SVolumesMount.getMountPath()).withReadOnly(k8SVolumesMount.isReadOnly()).build());
        }
        return volumeMounts;
    }

    private KubernetesClient getKubernetesClient() {
        return kubernetesClient == null ? new DefaultKubernetesClient(k8sConfig).inNamespace(namespace) : this.kubernetesClient;
    }

    public static class K8SVolumesMount {

        private String name;
        private String mountPath;
        private boolean readOnly;

        public K8SVolumesMount() {
        }

        @Override
        public String toString() {
            return "K8SVolumesMount{"
                    + "name='" + name + '\''
                    + ", mountPath='" + mountPath + '\''
                    + ", readOnly=" + readOnly
                    + '}';
        }

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
