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

import java.util.HashMap;
import java.util.Map;

public class FileInternal {

    private FileStatus status;
    private FileIndex index;
    private Map<String, String> sampleMap;
    private MissingSamples missingSamples;

    public FileInternal() {
    }

    public FileInternal(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
    }

    public FileInternal(FileStatus status, FileIndex index, Map<String, String> sampleMap, MissingSamples missingSamples) {
        this.status = status;
        this.index = index;
        this.sampleMap = sampleMap;
        this.missingSamples = missingSamples;
    }

    public static FileInternal initialize() {
        return new FileInternal(new FileStatus(FileStatus.READY), FileIndex.initialize(), new HashMap<>(), MissingSamples.initialize());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternal{");
        sb.append("status=").append(status);
        sb.append(", index=").append(index);
        sb.append(", sampleMap=").append(sampleMap);
        sb.append(", missingSamples=").append(missingSamples);
        sb.append('}');
        return sb.toString();
    }

    public FileStatus getStatus() {
        return status;
    }

    public FileInternal setStatus(FileStatus status) {
        this.status = status;
        return this;
    }

    public FileIndex getIndex() {
        return index;
    }

    public FileInternal setIndex(FileIndex index) {
        this.index = index;
        return this;
    }

    public Map<String, String> getSampleMap() {
        return sampleMap;
    }

    public FileInternal setSampleMap(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
        return this;
    }

    public MissingSamples getMissingSamples() {
        return missingSamples;
    }

    public FileInternal setMissingSamples(MissingSamples missingSamples) {
        this.missingSamples = missingSamples;
        return this;
    }
}
