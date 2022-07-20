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

package org.opencb.opencga.core.models.job;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.opencb.opencga.core.api.ParamConstants;

public class JobInternalWebhook implements Cloneable {

    @DataField(id = "webhook", indexed = true,
            description = FieldConstants.JOB_INTERNAL_WEBHOOK_URL_DESCRIPTION)
    private URL url;

    @DataField(id = "status", indexed = true, uncommentedClasses = {"Status"},
            description = FieldConstants.JOB_INTERNAL_WEBHOOK_STATUS_DESCRIPTION)
    private Map<String, Status> status;

    public JobInternalWebhook() {
    }

    public JobInternalWebhook(URL url, Map<String, Status> status) {
        this.url = url;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobInternalWebhook{");
        sb.append("webhook=").append(url);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public JobInternalWebhook clone() throws CloneNotSupportedException {
        JobInternalWebhook jobInternalWebhook = (JobInternalWebhook) super.clone();
        jobInternalWebhook.setStatus(new HashMap<>(status));
        return jobInternalWebhook;
    }

    public URL getUrl() {
        return url;
    }

    public JobInternalWebhook setUrl(URL url) {
        this.url = url;
        return this;
    }

    public Map<String, Status> getStatus() {
        return status;
    }

    public JobInternalWebhook setStatus(Map<String, Status> status) {
        this.status = status;
        return this;
    }

    public enum Status {
        SUCCESS,
        ERROR
    }
}
