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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;

import java.util.HashMap;
import java.util.Map;

import org.opencb.opencga.core.api.ParamConstants;

public class FileInternal extends Internal {

    @DataField(id = "status", indexed = true,
            description = FieldConstants.FILE_INTERNAL_STATUS_DESCRIPTION)
    private FileStatus status;

    @DataField(id = "variant", indexed = true,
            description = FieldConstants.FILE_INTERNAL_VARIANT_DESCRIPTION)
    private FileInternalVariant variant;

    @DataField(id = "alignment", indexed = true,
            description = FieldConstants.FILE_INTERNAL_ALIGNMENT_DESCRIPTION)
    private FileInternalAlignment alignment;

    @DataField(id = "sampleMap", indexed = true,
            description = FieldConstants.FILE_INTERNAL_SAMPLE_MAP_DESCRIPTION)
    private Map<String, String> sampleMap;

    @DataField(id = "missingSamples", indexed = true,
            description = FieldConstants.FILE_INTERNAL_MISSING_SAMPLE_DESCRIPTION)
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

    public static String getVariantIndexStatusId(FileInternal fileInternal) {
        String indexStatus;
        if (fileInternal == null
                || fileInternal.getVariant() == null
                || fileInternal.getVariant().getIndex() == null
                || fileInternal.getVariant().getIndex().getStatus() == null
                || fileInternal.getVariant().getIndex().getStatus().getId() == null) {
            indexStatus = VariantIndexStatus.NONE;
        } else {
            indexStatus = fileInternal.getVariant().getIndex().getStatus().getId();
        }
        return indexStatus;
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

    public FileInternalVariant getVariant() {
        return variant;
    }

    public FileInternal setVariant(FileInternalVariant variant) {
        this.variant = variant;
        return this;
    }

    public FileInternalAlignment getAlignment() {
        return alignment;
    }

    public FileInternal setAlignment(FileInternalAlignment alignment) {
        this.alignment = alignment;
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
