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

package org.opencb.opencga.core.models.file;

public class SmallFileInternal {

    private FileStatus status;
    private MissingSamples missingSamples;

    public SmallFileInternal() {
    }

    public SmallFileInternal(FileStatus status) {
        this.status = status;
    }

    public SmallFileInternal(FileStatus status, MissingSamples missingSamples) {
        this.status = status;
        this.missingSamples = missingSamples;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SmallFileInternal{");
        sb.append("status=").append(status);
        sb.append("missingSamples=").append(missingSamples);
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

    public MissingSamples getMissingSamples() {
        return missingSamples;
    }

    public SmallFileInternal setMissingSamples(MissingSamples missingSamples) {
        this.missingSamples = missingSamples;
        return this;
    }
}
