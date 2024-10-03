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

package org.opencb.opencga.core.models.common;

public class InternalQualityControl {

    private QualityControlStatus qualityControlStatus;

    public InternalQualityControl() {
    }

    public InternalQualityControl(QualityControlStatus qualityControlStatus) {
        this.qualityControlStatus = qualityControlStatus;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InternalQualityControl{");
        sb.append("qualityControlStatus=").append(qualityControlStatus);
        sb.append('}');
        return sb.toString();
    }

    public QualityControlStatus getQualityControlStatus() {
        return qualityControlStatus;
    }

    public InternalQualityControl setQualityControlStatus(QualityControlStatus qualityControlStatus) {
        this.qualityControlStatus = qualityControlStatus;
        return this;
    }
}
