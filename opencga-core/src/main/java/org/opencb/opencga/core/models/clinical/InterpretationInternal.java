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

package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;

public class InterpretationInternal extends Internal {

    @DataField(id = "status", indexed = true,
            description = FieldConstants.INTERPRETATION_INTERNAL_STATUS)
    private InterpretationStatus status;

    public InterpretationInternal() {
    }

    public InterpretationInternal(String registrationDate, String modificationDate, InterpretationStatus status) {
        super(null, registrationDate, modificationDate);
        this.status = status;
    }

    public static InterpretationInternal init() {
        return new InterpretationInternal(TimeUtils.getTime(), TimeUtils.getTime(), new InterpretationStatus());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", modificationDate='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public InterpretationStatus getStatus() {
        return status;
    }

    public InterpretationInternal setStatus(InterpretationStatus status) {
        this.status = status;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public InterpretationInternal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public InterpretationInternal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
