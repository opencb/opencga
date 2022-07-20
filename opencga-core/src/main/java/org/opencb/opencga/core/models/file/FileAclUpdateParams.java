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

import org.opencb.opencga.core.models.AclParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileAclUpdateParams extends AclParams {

    @DataField(description = ParamConstants.FILE_ACL_UPDATE_PARAMS_FILE_DESCRIPTION)
    private String file;
    @DataField(description = ParamConstants.FILE_ACL_UPDATE_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;

    public FileAclUpdateParams() {
    }

    public FileAclUpdateParams(String permissions, String file, String sample) {
        super(permissions);
        this.file = file;
        this.sample = sample;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileAclUpdateParams{");
        sb.append("file='").append(file).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public FileAclUpdateParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public FileAclUpdateParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public FileAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
