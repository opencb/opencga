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

package org.opencb.opencga.core.config;

/**
 * Created by wasim on 13/02/19.
 */
public class HealthCheck {
    private int interval;

    public int getInterval() {
        return interval;
    }

    public HealthCheck setInterval(int interval) {
        this.interval = interval;
        return this;
    }

    public HealthCheck() {
        interval = 30;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HealthCheck{");
        sb.append("interval=").append(interval);
        sb.append('}');
        return sb.toString();
    }
}
