package org.opencb.opencga.core.models.user;

public class UserQuota {

    /**
     * Current disk usage of user in bytes.
     */
    private long diskUsage;
    /**
     * Current cpu usage in seconds.
     */
    private int cpuUsage;
    /**
     * Maximum amount of disk in bytes allowed for the user to use.
     */
    private long maxDisk;
    /**
     * Maximum amount of seconds the user can use of CPU.
     */
    private int maxCpu;

    public UserQuota() {
        this(-1, -1, -1, -1);
    }

    public UserQuota(long diskUsage, int cpuUsage, long maxDisk, int maxCpu) {
        this.diskUsage = diskUsage;
        this.cpuUsage = cpuUsage;
        this.maxDisk = maxDisk;
        this.maxCpu = maxCpu;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserQuota{");
        sb.append("diskUsage=").append(diskUsage);
        sb.append(", cpuUsage=").append(cpuUsage);
        sb.append(", maxDisk=").append(maxDisk);
        sb.append(", maxCpu=").append(maxCpu);
        sb.append('}');
        return sb.toString();
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public UserQuota setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
        return this;
    }

    public int getCpuUsage() {
        return cpuUsage;
    }

    public UserQuota setCpuUsage(int cpuUsage) {
        this.cpuUsage = cpuUsage;
        return this;
    }

    public long getMaxDisk() {
        return maxDisk;
    }

    public UserQuota setMaxDisk(long maxDisk) {
        this.maxDisk = maxDisk;
        return this;
    }

    public int getMaxCpu() {
        return maxCpu;
    }

    public UserQuota setMaxCpu(int maxCpu) {
        this.maxCpu = maxCpu;
        return this;
    }
}
