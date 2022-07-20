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

package org.opencb.opencga.core.models.user;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class UserQuota {

    /**
     * Current disk usage of user in bytes.
     */
    @DataField(description = ParamConstants.USER_QUOTA_DISK_USAGE_DESCRIPTION)
    private long diskUsage;
    /**
     * Current cpu usage in seconds.
     */
    @DataField(description = ParamConstants.USER_QUOTA_CPU_USAGE_DESCRIPTION)
    private int cpuUsage;
    /**
     * Maximum amount of disk in bytes allowed for the user to use.
     */
    @DataField(description = ParamConstants.USER_QUOTA_MAX_DISK_DESCRIPTION)
    private long maxDisk;
    /**
     * Maximum amount of seconds the user can use of CPU.
     */
    @DataField(description = ParamConstants.USER_QUOTA_MAX_CPU_DESCRIPTION)
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
