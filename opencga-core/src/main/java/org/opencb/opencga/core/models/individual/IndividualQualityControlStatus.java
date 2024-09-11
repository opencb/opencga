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

package org.opencb.opencga.core.models.individual;

import org.opencb.opencga.core.models.common.QualityControlStatus;

import java.util.Objects;

public class IndividualQualityControlStatus extends QualityControlStatus {

    private int code;

    public static final int NONE_READY = 0;
    public static final int INFERRED_SEX_READY = 1;
    public static final int MENDELIAN_ERROR_READY = 2;
    public static final int RELATEDNESS_READY = 4;

    public IndividualQualityControlStatus(int code, String message) {
        String status;
        switch (code) {
            case NONE_READY:
                status = NONE;
                break;
            case INFERRED_SEX_READY + MENDELIAN_ERROR_READY + RELATEDNESS_READY:
                status = READY;
                break;
            default:
                status = INCOMPLETE;
                break;
        }
        this.code = code;
        init(status, status, message);
    }

    public IndividualQualityControlStatus(int code) {
        this(code, "");
    }

    public IndividualQualityControlStatus() {
        this(NONE_READY, "");
    }

    public IndividualQualityControlStatus(String status, String message) {
        super(status, message);
    }

    public static IndividualQualityControlStatus init() {
        return new IndividualQualityControlStatus();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualQualityControlStatus{");
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

    public IndividualQualityControlStatus setCode(int code) {
        this.code = code;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndividualQualityControlStatus)) return false;
        if (!super.equals(o)) return false;

        IndividualQualityControlStatus that = (IndividualQualityControlStatus) o;

        return Objects.equals(code, that.code);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + code;
        return result;
    }
}
