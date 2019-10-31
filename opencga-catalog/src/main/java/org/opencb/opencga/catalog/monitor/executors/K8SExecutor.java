package org.opencb.opencga.catalog.monitor.executors;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.K8SVolumesMount;
import org.opencb.opencga.core.models.Job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


public class K8SExecutor implements BatchExecutor {

    public static final String K8S_KIND = "Job";

    private String k8sClusterMaster;
    private String namespace;
    private String imageName;
    private String cpu;
    private String memory;
    private List<VolumeMount> volumeMounts;
    private Config k8sConfig;
    private KubernetesClient kubernetesClient;

    public K8SExecutor(Configuration configuration) {
        this.k8sClusterMaster = configuration.getExecution().getK8sMasterNode();
        this.namespace = configuration.getExecution().getNamespace();
        this.imageName = configuration.getExecution().getImageName();
        this.cpu = configuration.getExecution().getCpu();
        this.memory = configuration.getExecution().getMemory();
        this.volumeMounts = buildVolumeMounts(configuration.getExecution().getK8SVolumesMount());
        this.k8sConfig = new ConfigBuilder().withMasterUrl(k8sClusterMaster).build();
        this.kubernetesClient = new DefaultKubernetesClient(k8sConfig).inNamespace(namespace);
    }

    @Override
    public void execute(Job job, String token) throws Exception {
        HashMap<String, Quantity> requests = new HashMap();
        requests.put("cpu", new Quantity(this.cpu));
        requests.put("memory", new Quantity(this.memory));

        final io.fabric8.kubernetes.api.model.batch.Job k8sJob = new JobBuilder()
                    .withApiVersion("batch/v1")
                    .withKind(K8S_KIND)
                    .withNewMetadata()
                        .withName("opencga-job-" + job.getId().replace("_", "-"))
                        .withLabels(Collections.singletonMap("opencga", "job"))
                        .withAnnotations(Collections.singletonMap("variantFileSize", Long.toString(job.getSize())))
                    .endMetadata()
                    .withNewSpec()
                    .withTtlSecondsAfterFinished(30)
                    .withNewTemplate()
                    .withNewSpec()
                    .addNewContainer()
                    .withName("opencga-job-" + job.getId().replace("_", "-"))
                    .withImage(this.imageName)
                    .withImagePullPolicy("Always")
                    .withResources(new ResourceRequirementsBuilder().withRequests(requests).build())
                    .withArgs("/bin/sh", "-c", getCommandLine(job, token))
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

    private List<VolumeMount> buildVolumeMounts(List<K8SVolumesMount> k8SVolumesMounts) {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        for (K8SVolumesMount k8SVolumesMount : k8SVolumesMounts) {
            volumeMounts.add(new VolumeMountBuilder().withName(k8SVolumesMount.getName())
                    .withMountPath(k8SVolumesMount.getMountPath()).withReadOnly(k8SVolumesMount.isReadOnly()).build());
        }
        return volumeMounts;
    }

    private KubernetesClient getKubernetesClient() {
        return kubernetesClient == null ? new DefaultKubernetesClient(k8sConfig).inNamespace(namespace) : this.kubernetesClient;
    }
}
