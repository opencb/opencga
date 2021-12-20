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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.*;

/**
 * Created by pfurio on 11/03/16.
 */
public class Status {


    /**
     * READY name means that the object is being used.
     */
    public static final String READY = "READY";
    /**
     * DELETED name means that the object is marked as removed, so it can be completely removed from the database with a clean action.
     */
    public static final String DELETED = "DELETED";
    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED);
    @DataField(id = "name", indexed = true,
            description = FieldConstants.GENERIC_NAME + " Status")
    private String name;
    private String date;
    private String description;
    @Deprecated
    private String message;

    public Status() {
        this(READY, "");
    }

    public Status(String name) {
        this(name, "");
    }

    public Status(String name, String description) {
        if (isValid(name)) {
            init(name, description);
        } else {
            throw new IllegalArgumentException("Unknown name '" + name + "'");
        }
    }

    public static boolean isValid(String status) {
        return status != null && (status.equals(READY) || status.equals(DELETED));
    }

    public static String getPositiveStatus(List<String> acceptedStatusList, String status) {
        List<String> positiveStatusList = new ArrayList<>();
        List<String> negativeStatusList = new ArrayList<>();

        String[] andSplit = status.split(";");
        if (andSplit.length > 1) {
            fillPositiveNegativeList(andSplit, positiveStatusList, negativeStatusList);
        } else {
            String[] orSplit = status.split(",");
            fillPositiveNegativeList(orSplit, positiveStatusList, negativeStatusList);
        }

        if (!positiveStatusList.isEmpty()) {
            return StringUtils.join(positiveStatusList, ",");
        } else if (!negativeStatusList.isEmpty()) {
            Set<String> allStatusSet = new HashSet<>(acceptedStatusList);
            for (String s : negativeStatusList) {
                allStatusSet.remove(s);
            }
            if (!allStatusSet.isEmpty()) {
                return StringUtils.join(allStatusSet, ",");
            } else {
                return StringUtils.join(acceptedStatusList, ",");
            }
        } else {
            return StringUtils.join(acceptedStatusList, ",");
        }
    }

    private static void fillPositiveNegativeList(String[] statusList, List<String> positiveStatusList, List<String> negativeStatusList) {
        for (String s : statusList) {
            if (s.startsWith("!=")) {
                negativeStatusList.add(s.replace("!=", ""));
            } else if (s.startsWith("!")) {
                negativeStatusList.add(s.replace("!", ""));
            } else {
                positiveStatusList.add(s);
            }
        }
    }

    protected void init(String status, String description) {
        this.name = status;
        this.date = TimeUtils.getTime();
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Status)) {
            return false;
        }
        Status status = (Status) o;
        return Objects.equals(name, status.name)
                && Objects.equals(date, status.date)
                && Objects.equals(description, status.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, date, description);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Status{");
        sb.append("name='").append(name).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public Status setName(String name) {
        this.name = name;
        return this;
    }

    public String getDate() {
        return date;
    }

    public Status setDate(String date) {
        this.date = date;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Status setDescription(String description) {
        this.description = description;
        return this;
    }
}
