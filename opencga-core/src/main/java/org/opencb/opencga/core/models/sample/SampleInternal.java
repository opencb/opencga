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

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.RgaIndex;
import org.opencb.opencga.core.models.common.Status;

/**
 * Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh interdum
 * finibus.
 */
public class SampleInternal extends Internal {

    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     * @deprecated
     */
    private RgaIndex rga;

    public SampleInternal() {
    }

    public SampleInternal(String registrationDate, String modificationDate, Status status, RgaIndex rga) {
        super(status, registrationDate, modificationDate);
        this.rga = rga;
    }

    public static SampleInternal init() {
        return new SampleInternal(TimeUtils.getTime(), TimeUtils.getTime(), new Status(Status.READY), RgaIndex.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternal{");
        sb.append("status=").append(status);
        sb.append(", rga=").append(rga);
        sb.append('}');
        return sb.toString();
    }

    public Status getStatus() {
        return status;
    }

    public SampleInternal setStatus(Status status) {
        this.status = status;
        return this;
    }

    public RgaIndex getRga() {
        return rga;
    }

    public SampleInternal setRga(RgaIndex rga) {
        this.rga = rga;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public SampleInternal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public SampleInternal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
