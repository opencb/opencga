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

package org.opencb.opencga.core.models.study;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.net.URL;

import org.opencb.opencga.core.api.ParamConstants;

public class StudyNotification {


    @DataField(id = "webhook", indexed = true,
            description = FieldConstants.STUDY_NOTIFICATION_WEBHOOK)
    private URL webhook;

    public StudyNotification() {
    }

    public StudyNotification(URL webhook) {
        this.webhook = webhook;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyNotification{");
        sb.append("webhook=").append(webhook);
        sb.append('}');
        return sb.toString();
    }

    public URL getWebhook() {
        return webhook;
    }

    public StudyNotification setWebhook(URL webhook) {
        this.webhook = webhook;
        return this;
    }
}
