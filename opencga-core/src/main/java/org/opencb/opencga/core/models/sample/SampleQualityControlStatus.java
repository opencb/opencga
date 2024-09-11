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

package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.QualityControlStatus;

import java.util.Objects;

public class SampleQualityControlStatus extends QualityControlStatus {

    private int code;

    public static final int NONE_READY = 0;
    public static final int SIGNATURE_READY = 1;
    public static final int HR_DETECT_READY = 2;
    public static final int GENOME_PLOT_READY = 4;

    public SampleQualityControlStatus(int code, String message) {
        String status;
        switch (code) {
            case NONE_READY:
                status = NONE;
                break;
            case SIGNATURE_READY + HR_DETECT_READY + GENOME_PLOT_READY:
                status = READY;
                break;
            default:
                status = INCOMPLETE;
                break;
        }
        this.code = code;
        init(status, status, message);
    }

    public SampleQualityControlStatus(int code) {
        this(code, "");
    }

    public SampleQualityControlStatus() {
        this(NONE_READY, "");
    }

    public SampleQualityControlStatus(String status, String message) {
        super(status, message);
    }

    public static SampleQualityControlStatus init() {
        return new SampleQualityControlStatus();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQualityControlStatus{");
        sb.append("code=").append(code);
        sb.append(", id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public int getCode() {
        return code;
    }

    public SampleQualityControlStatus setCode(int code) {
        this.code = code;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SampleQualityControlStatus)) return false;
        if (!super.equals(o)) return false;

        SampleQualityControlStatus that = (SampleQualityControlStatus) o;

        return Objects.equals(code, that.code);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + code;
        return result;
    }
}
