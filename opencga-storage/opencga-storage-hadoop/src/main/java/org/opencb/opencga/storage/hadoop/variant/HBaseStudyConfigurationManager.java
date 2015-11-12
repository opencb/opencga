package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.mr.GenomeHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created on 12/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseStudyConfigurationManager extends StudyConfigurationManager {

    private HadoopCredentials credentials;
    private Configuration configuration;
    private ObjectMap options;
    private GenomeHelper genomeHelper;
    private Connection connection;

    HBaseStudyConfigurationManager(ObjectMap options) {
        super(options);
//        throw new UnsupportedOperationException();
    }

    public HBaseStudyConfigurationManager(HadoopCredentials credentials, Configuration configuration, ObjectMap options)
            throws IOException {
        super(options);
        this.credentials = credentials;
        this.configuration = configuration;
        this.options = options;
        genomeHelper = new GenomeHelper(configuration);
        connection = ConnectionFactory.createConnection(configuration);
    }

    @Override
    protected QueryResult<StudyConfiguration> _getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {

        return null;
    }

    @Override
    protected QueryResult<StudyConfiguration> _getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        return null;
    }

    @Override
    protected QueryResult _updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        return null;
    }

    @Override
    public List<String> getStudyNames(QueryOptions options) {
        return Collections.emptyList();
    }
}
