package org.opencb.opencga.storage.core.metadata;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.time.Instant;
import java.util.List;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ExportMetadata {
    private final List<StudyConfiguration> studies;
    private final String date;
    private final Query query;
    private final String version;
    private final String gitVersion;

    public ExportMetadata(List<StudyConfiguration> studies, Query query) {
        this.studies = studies;
        date = Instant.now().toString();
        this.query = query;
        version = GitRepositoryState.get().getDescribeShort();
        gitVersion = GitRepositoryState.get().getCommitId();
    }

    public ExportMetadata(List<StudyConfiguration> studies, String date, Query query, String version, String gitVersion) {
        this.studies = studies;
        this.date = date;
        this.query = query;
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

    public String getVersion() {
        return version;
    }

    public String getGitVersion() {
        return gitVersion;
    }
}
