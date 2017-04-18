package org.opencb.opencga.storage.core.metadata;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class StudyConfigurationAdaptor implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(StudyConfigurationAdaptor.class);

    protected long lockStudy(int studyId, long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        logger.warn("Ignoring lock");
        return 0;
    }

    protected void unLockStudy(int studyId, long lockId) {
        logger.warn("Ignoring unLock");
    }

    protected abstract QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, Long time, QueryOptions options);

    protected abstract QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options);

    protected abstract QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options);

    protected abstract Map<String, Integer> getStudies(QueryOptions options);

    protected List<String> getStudyNames(QueryOptions options) {
        return new ArrayList<>(getStudies(options).keySet());
    }

    protected List<Integer> getStudyIds(QueryOptions options) {
        return new ArrayList<>(getStudies(options).values());
    }

    @Override
    public void close() throws IOException {

    }
}
