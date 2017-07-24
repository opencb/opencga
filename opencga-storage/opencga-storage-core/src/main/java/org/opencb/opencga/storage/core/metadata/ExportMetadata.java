/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.metadata;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ExportMetadata {
    private final List<StudyConfiguration> studies;
    private final String date;
    private final Query query;
    private final QueryOptions queryOptions;
    private final String version;
    private final String gitVersion;

    // TODO:
    // private final long numVariants;

    ExportMetadata() {
        studies = null;
        date = null;
        query = null;
        queryOptions = null;
        version = null;
        gitVersion = null;
    }

    public ExportMetadata(List<StudyConfiguration> studies, Query query, QueryOptions queryOptions) {
        this.studies = studies;
        this.queryOptions = queryOptions;
        date = Instant.now().toString();
        this.query = query;
        version = GitRepositoryState.get().getDescribeShort();
        gitVersion = GitRepositoryState.get().getCommitId();
    }

    public ExportMetadata(List<StudyConfiguration> studies, String date, Query query, QueryOptions queryOptions,
                          String version, String gitVersion) {
        this.studies = studies;
        this.date = date;
        this.query = query;
        this.queryOptions = queryOptions;
        this.version = version;
        this.gitVersion = gitVersion;
    }

    public List<StudyConfiguration> getStudies() {
        return studies;
    }

    public String getDate() {
        return date;
    }

    public Query getQuery() {
        return query;
    }

    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    public String getVersion() {
        return version;
    }

    public String getGitVersion() {
        return gitVersion;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExportMetadata{");
        sb.append("studies=").append(studies);
        sb.append(", date='").append(date).append('\'');
        sb.append(", query=").append(query);
        sb.append(", queryOptions=").append(queryOptions);
        sb.append(", version='").append(version).append('\'');
        sb.append(", gitVersion='").append(gitVersion).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExportMetadata)) {
            return false;
        }
        ExportMetadata that = (ExportMetadata) o;
        return Objects.equals(studies, that.studies)
                && Objects.equals(date, that.date)
                && Objects.equals(query, that.query)
                && Objects.equals(queryOptions, that.queryOptions)
                && Objects.equals(version, that.version)
                && Objects.equals(gitVersion, that.gitVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(studies, date, query, queryOptions, version, gitVersion);
    }
}
