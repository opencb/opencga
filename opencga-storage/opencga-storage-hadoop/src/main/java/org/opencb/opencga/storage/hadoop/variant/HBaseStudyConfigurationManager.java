package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.io.IOException;
import java.util.*;

/**
 * Created on 12/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseStudyConfigurationManager extends StudyConfigurationManager {

    public static final byte[] STUDIES_ROW = Bytes.toBytes("STUDIES");
    public static final byte[] STUDIES_SUMMARY_COLUMN = Bytes.toBytes("SUMMARY");
    final byte[] columnFamily;

    private final HadoopCredentials credentials;
    private final Configuration configuration;
    private final ObjectMap options;
    private final GenomeHelper genomeHelper;
    private Connection connection;
    private final ObjectMapper objectMapper;

    private final HBaseManager hBaseManager;

    public HBaseStudyConfigurationManager(HadoopCredentials credentials, Configuration configuration, ObjectMap options)
            throws IOException {
        super(options);
        this.credentials = credentials;
        this.configuration = configuration;
        this.options = options;
        genomeHelper = new GenomeHelper(configuration);
        columnFamily = genomeHelper.getColumnFamily();
        connection = null; // lazy load
        objectMapper = new ObjectMapper();
        hBaseManager = new HBaseManager(configuration);
    }

    @Override
    protected QueryResult<StudyConfiguration> _getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        return _getStudyConfiguration(getStudiesSummary(options).inverse().get(studyId), timeStamp, options);
    }

    @Override
    protected QueryResult<StudyConfiguration> _getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = null;
        List<StudyConfiguration> studyConfigurationList = Collections.emptyList();
        logger.info("Get StudyConfiguration {}", studyName);
        Get get = new Get(STUDIES_ROW);
        byte[] columnQualifier = Bytes.toBytes(studyName);
        get.addColumn(columnFamily, columnQualifier);
        if (timeStamp != null) {
            try {
                get.setTimeRange(timeStamp + 1, Long.MAX_VALUE);
            } catch (IOException e) {
                //This should not happen never.
                throw new IllegalArgumentException(e);
            }
        }

        try {
            if (!hBaseManager.act(getConnection(), credentials.getTable(), (table, admin) -> admin.tableExists(table.getName()))) {
                studyConfigurationList = hBaseManager.act(getConnection(), credentials.getTable(), table -> {
                    Result result = table.get(get);
                    if (result.isEmpty()) {
                        return Collections.emptyList();
                    } else {
                        byte[] value = result.getValue(columnFamily, columnQualifier);
                        StudyConfiguration studyConfiguration = objectMapper.readValue(value, StudyConfiguration.class);
                        return Collections.singletonList(studyConfiguration);
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
            error = e.getMessage();
        }
        return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime),
                studyConfigurationList.size(), studyConfigurationList.size(), "", error, studyConfigurationList);
    }

    @Override
    protected QueryResult _updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = "";
        logger.info("Update StudyConfiguration {}", studyConfiguration.getStudyName());
        updateStudiesSummary(studyConfiguration.getStudyName(), studyConfiguration.getStudyId(), options);
        byte[] columnQualifier = Bytes.toBytes(studyConfiguration.getStudyName());

        try {
            hBaseManager.act(getConnection(), credentials.getTable(), table -> {
                byte[] bytes = objectMapper.writeValueAsBytes(studyConfiguration);
                Put put = new Put(STUDIES_ROW);
                put.addColumn(columnFamily, columnQualifier, studyConfiguration.getTimeStamp(), bytes);
                table.put(put);
            });
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
        get.addColumn(columnFamily, STUDIES_SUMMARY_COLUMN);
        try {
            if (!hBaseManager.act(getConnection(), credentials.getTable(), (table, admin) -> admin.tableExists(table.getName()))) {
                return HashBiMap.create();
            }
            return hBaseManager.act(getConnection(), credentials.getTable(), table -> {
                Result result = table.get(get);
                if (result.isEmpty()) {
                    return HashBiMap.create();
                } else {
                    byte[] value = result.getValue(columnFamily, STUDIES_SUMMARY_COLUMN);
                    Map<String, Integer> map = objectMapper.readValue(value, Map.class);
                    logger.info("Get StudyConfiguration summary {}", map);

                    return HashBiMap.create(map);
                }
            });
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
        try {
            createTableIfMissing();
            try(Table table = getConnection().getTable(TableName.valueOf(credentials.getTable()))) {
                byte[] bytes = objectMapper.writeValueAsBytes(studies);
                Put put = new Put(STUDIES_ROW);
                put.addColumn(columnFamily, STUDIES_SUMMARY_COLUMN, bytes);
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean createTableIfMissing() throws IOException {
        return hBaseManager.act(getConnection(), credentials.getTable(), (table, admin) -> {
            if (admin.tableExists(table.getName())) {
                return true;
            } else {
                admin.createTable(new HTableDescriptor(TableName.valueOf(credentials.getTable())));
                return false;
            }
        });
    }

    public Connection getConnection() throws IOException {
        if(null == connection){
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }

}
