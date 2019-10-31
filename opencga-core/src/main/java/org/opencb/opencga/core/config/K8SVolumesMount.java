package org.opencb.opencga.core.config;

public class K8SVolumesMount {

    private String name;
    private String mountPath;
    private boolean readOnly;

    public K8SVolumesMount() {
    }

    @Override
    public String toString() {
        return "K8SVolumesMount{" +
                "name='" + name + '\'' +
                ", mountPath='" + mountPath + '\'' +
                ", readOnly=" + readOnly +
                '}';
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
