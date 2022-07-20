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
import org.opencb.opencga.core.api.ParamConstants;

public class Alert {

    @DataField(description = ParamConstants.ALERT_AUTHOR_DESCRIPTION)
    private String author;
    @DataField(description = ParamConstants.ALERT_DATE_DESCRIPTION)
    private String date;
    @DataField(description = ParamConstants.ALERT_MESSAGE_DESCRIPTION)
    private String message;
    @DataField(description = ParamConstants.ALERT_RISK_DESCRIPTION)
    private Risk risk;

    public Alert() {
    }

    public Alert(String author, String date, String message, Risk risk) {
        this.author = author;
        this.date = date;
        this.message = message;
        this.risk = risk;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Alert{");
        sb.append("author='").append(author).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", risk=").append(risk);
        sb.append('}');
        return sb.toString();
    }

    public String getAuthor() {
        return author;
    }

    public Alert setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getDate() {
        return date;
    }

    public Alert setDate(String date) {
        this.date = date;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public Alert setMessage(String message) {
        this.message = message;
        return this;
    }

    public Risk getRisk() {
        return risk;
    }

    public Alert setRisk(Risk risk) {
        this.risk = risk;
        return this;
    }

    public enum Risk {
        HIGH,
        MEDIUM,
        LOW
    }

}
