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

package org.opencb.opencga.core.models.cohort;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class CohortInternal extends Internal {

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private CohortStatus status;

    public CohortInternal() {
    }

    public CohortInternal(String registrationDate, String modificationDate, CohortStatus status) {
        super(null, registrationDate, modificationDate);
        this.status = status;
    }

    public static CohortInternal init() {
        return new CohortInternal(TimeUtils.getTime(), TimeUtils.getTime(), new CohortStatus());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public CohortStatus getStatus() {
        return status;
    }

    public CohortInternal setStatus(CohortStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohortInternal that = (CohortInternal) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }
}
