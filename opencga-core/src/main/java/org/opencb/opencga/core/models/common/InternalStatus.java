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
import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.*;

/**
 * Created by pfurio on 11/03/16.
 */
public class InternalStatus extends Status {

    private String version;
    private String commit;

    /**
     * READY name means that the object is being used.
     */
    public static final String READY = "READY";

    /**
     * DELETED name means that the object is marked as removed, so it can be completely removed from the database with a clean action.
     */
    public static final String DELETED = "DELETED";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED);


    public InternalStatus() {
        this(READY, "");
    }

    public InternalStatus(String id) {
        this(id, "");
    }

    public InternalStatus(String id, String description) {
        if (isValid(id)) {
            init(id, description);
        } else {
            throw new IllegalArgumentException("Unknown status id '" + id + "'");
        }
    }

    public InternalStatus(String id, String name, String description, String date, String version, String commit) {
        super(id, name, description, date);
        if (!isValid(id)) {
            throw new IllegalArgumentException("Unknown status id '" + id + "'");
        }
        this.version = version;
        this.commit = commit;
    }

    protected void init(String status, String description) {
        setId(status);
        setDescription(description);
        setDate(TimeUtils.getTime());
        this.version = GitRepositoryState.get().getBuildVersion();
        this.commit = GitRepositoryState.get().getCommitId();
    }

    public static boolean isValid(String status) {
        if (status != null && (status.equals(READY) || status.equals(DELETED))) {
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InternalStatus)) return false;
        if (!super.equals(o)) return false;

        InternalStatus that = (InternalStatus) o;

        if (version != null ? !version.equals(that.version) : that.version != null) return false;
        return commit != null ? commit.equals(that.commit) : that.commit == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (commit != null ? commit.hashCode() : 0);
        return result;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InternalStatus{");
        sb.append("version='").append(version).append('\'');
        sb.append(", commit='").append(commit).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getVersion() {
        return version;
    }

    public InternalStatus setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getCommit() {
        return commit;
    }

    public InternalStatus setCommit(String commit) {
        this.commit = commit;
        return this;
    }

    public String getId() {
        return id;
    }

    public InternalStatus setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public InternalStatus setName(String name) {
        this.name = name;
        return this;
    }

    public String getDate() {
        return date;
    }

    public InternalStatus setDate(String date) {
        this.date = date;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public InternalStatus setDescription(String description) {
        this.description = description;
        return this;
    }
}