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

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;

import java.util.HashMap;
import java.util.Map;

public class FileInternal extends Internal {

    private FileStatus status;
    private FileInternalVariant variant;
    private FileInternalAlignment alignment;
    private Map<String, String> sampleMap;
    private MissingSamples missingSamples;

    public FileInternal() {
    }

    public FileInternal(String registrationDate, String modificationDate, FileStatus status, FileInternalVariant variant,
                        FileInternalAlignment alignment, Map<String, String> sampleMap, MissingSamples missingSamples) {
        super(null, registrationDate, modificationDate);
        this.status = status;
        this.variant = variant;
        this.alignment = alignment;
        this.sampleMap = sampleMap;
        this.missingSamples = missingSamples;
    }

    public static FileInternal init() {
        return new FileInternal(TimeUtils.getTime(), TimeUtils.getTime(), new FileStatus(FileStatus.READY), FileInternalVariant.init(),
                FileInternalAlignment.init(), new HashMap<>(), MissingSamples.initialize());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternal{");
        sb.append("registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append(", status=").append(status);
        sb.append(", variant=").append(variant);
        sb.append(", alignment=").append(alignment);
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

    // TODO: REMOVE
    public FileIndex getIndex() {
        return null;
    }

    // TODO: REMOVE
    public FileInternal setIndex(FileIndex index) {
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
