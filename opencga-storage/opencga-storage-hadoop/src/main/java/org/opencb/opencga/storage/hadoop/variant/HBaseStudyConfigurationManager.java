package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.mr.GenomeHelper;

import java.io.IOException;
import java.util.*;

/**
 * Created on 12/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseStudyConfigurationManager extends StudyConfigurationManager {

    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    public static final byte[] STUDIES_ROW = Bytes.toBytes("STUDIES");
    public static final byte[] STUDIES_SUMMARY_COLUMN = Bytes.toBytes("SUMMARY");

    private HadoopCredentials credentials;
    private Configuration configuration;
    private ObjectMap options;
    private GenomeHelper genomeHelper;
    private Connection connection;
    private final ObjectMapper objectMapper;

    HBaseStudyConfigurationManager(ObjectMap options) {
        super(options);
        throw new UnsupportedOperationException();
    }

    public HBaseStudyConfigurationManager(HadoopCredentials credentials, Configuration configuration, ObjectMap options)
            throws IOException {
        super(options);
        this.credentials = credentials;
        this.configuration = configuration;
        this.options = options;
        genomeHelper = new GenomeHelper(configuration);
        connection = ConnectionFactory.createConnection(configuration);
        objectMapper = new ObjectMapper();
    }

    @Override
    protected QueryResult<StudyConfiguration> _getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        return _getStudyConfiguration(getStudiesSummary(options).inverse().get(studyId), timeStamp, options);
    }

    @Override
    protected QueryResult<StudyConfiguration> _getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error;
        logger.info("Get StudyConfiguration {}", studyName);
        Get get = new Get(STUDIES_ROW);
        byte[] columnQualifier = Bytes.toBytes(studyName);
        get.addColumn(COLUMN_FAMILY, columnQualifier);
        if (timeStamp != null) {
            try {
                get.setTimeRange(timeStamp + 1, Long.MAX_VALUE);
            } catch (IOException e) {
                //This should not happen never.
                throw new IllegalArgumentException(e);
            }
        }
        try(Table table = connection.getTable(TableName.valueOf(credentials.getTable()))) {
            Result result = table.get(get);
            if (result.isEmpty()) {
                return new QueryResult<>();
            } else {
                byte[] value = result.getValue(COLUMN_FAMILY, columnQualifier);
                StudyConfiguration studyConfiguration = objectMapper.readValue(value, StudyConfiguration.class);
                return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime), 1, 1, "", "",
                        Collections.singletonList(studyConfiguration));
            }
        } catch (IOException e) {
            e.printStackTrace();
            error = e.getMessage();
        }
        return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime), 0, 0, "", error, Collections.emptyList());
    }

    @Override
    protected QueryResult _updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = "";
        logger.info("Update StudyConfiguration {}", studyConfiguration.getStudyName());
        updateStudiesSummary(studyConfiguration.getStudyName(), studyConfiguration.getStudyId(), options);
        byte[] columnQualifier = Bytes.toBytes(studyConfiguration.getStudyName());

        try(Table table = connection.getTable(TableName.valueOf(credentials.getTable()))) {
            byte[] bytes = objectMapper.writeValueAsBytes(studyConfiguration);
            Put put = new Put(STUDIES_ROW);
            put.addColumn(COLUMN_FAMILY, columnQualifier, studyConfiguration.getTimeStamp(), bytes);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            error = e.getMessage();
        }

        return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime), 0, 0, "", error, Collections.emptyList());
    }

    @Override
    public List<String> getStudyNames(QueryOptions options) {
        return new ArrayList<>(getStudiesSummary(options).keySet());
    }

    private BiMap<String, Integer> getStudiesSummary(QueryOptions options) {
        Get get = new Get(STUDIES_ROW);
        get.addColumn(COLUMN_FAMILY, STUDIES_SUMMARY_COLUMN);
        try(Table table = connection.getTable(TableName.valueOf(credentials.getTable()))) {
            Result result = table.get(get);
            if (result.isEmpty()) {
                return HashBiMap.create();
            } else {
                byte[] value = result.getValue(COLUMN_FAMILY, STUDIES_SUMMARY_COLUMN);
                Map<String, Integer> map = objectMapper.readValue(value, Map.class);
                logger.info("Get StudyConfiguration summary {}", map);

                return HashBiMap.create(map);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return HashBiMap.create();
        }
    }

    private void updateStudiesSummary(String study, Integer studyId, QueryOptions options) {
        BiMap<String, Integer> studiesSummary = getStudiesSummary(options);
        if (studiesSummary.getOrDefault(study, -1).equals(studyId)) {
            //Nothing to update
            return;
        } else {
            studiesSummary.put(study, studyId);
            updateStudiesSummary(studiesSummary, options);
        }
    }

    private void updateStudiesSummary(BiMap<String, Integer> studies, QueryOptions options) {
        try(Table table = connection.getTable(TableName.valueOf(credentials.getTable()))) {
            byte[] bytes = objectMapper.writeValueAsBytes(studies);
            Put put = new Put(STUDIES_ROW);
            put.addColumn(COLUMN_FAMILY, STUDIES_SUMMARY_COLUMN, bytes);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
