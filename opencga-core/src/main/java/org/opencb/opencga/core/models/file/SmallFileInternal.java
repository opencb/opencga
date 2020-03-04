package org.opencb.opencga.core.models.file;

public class SmallFileInternal {

    private FileStatus status;

    public SmallFileInternal() {
    }

    public SmallFileInternal(FileStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SmallFileInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public FileStatus getStatus() {
        return status;
    }

    public SmallFileInternal setStatus(FileStatus status) {
        this.status = status;
        return this;
    }
}
