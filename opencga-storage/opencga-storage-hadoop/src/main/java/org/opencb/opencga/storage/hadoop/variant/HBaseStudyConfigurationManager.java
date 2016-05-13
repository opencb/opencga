package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.metadata.BatchFileOperation;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStudyConfiguration;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created on 12/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseStudyConfigurationManager extends StudyConfigurationManager {

    private final byte[] studiesRow;
    private final byte[] studiesSummaryColumn;

    private final Configuration configuration;
    private final ObjectMap options;
    private final GenomeHelper genomeHelper;
    private final ObjectMapper objectMapper;
    private final String tableName;

    public HBaseStudyConfigurationManager(GenomeHelper helper, String tableName, Configuration configuration, ObjectMap options)
            throws IOException {
        super(options);
        this.configuration = Objects.requireNonNull(configuration);
        this.tableName = Objects.requireNonNull(tableName);
        this.options = options;
        this.genomeHelper = helper;
        this.objectMapper = new ObjectMapper();
        this.studiesRow = genomeHelper.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
        this.studiesSummaryColumn = genomeHelper.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
    }

    public HBaseStudyConfigurationManager(String tableName, Configuration configuration, ObjectMap options)
            throws IOException {
        this(new GenomeHelper(configuration), tableName, configuration, options);
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        logger.info("Get StudyConfiguration " + studyId + " from DB " + tableName);
        return internalGetStudyConfiguration(getStudiesSummary(options).inverse().get(studyId), timeStamp, options);
    }

    //FIXME: Use HBase lock
    public static ReentrantLock lock = new ReentrantLock();



    @Override
    public long lockStudy(int studyId, long lockDuration, long timeout) throws InterruptedException {
        boolean hasLock = lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        if (! hasLock) {
            throw new IllegalStateException("Can't get lock !!!");
        }
        return 0;
    }

    @Override
    public void unLockStudy(int studyId, long lockId) {
        lock.unlock();
    }


    public GenomeHelper getGenomeHelper() {
        return this.genomeHelper;
    }

    public HBaseManager getHBaseHelper() {
        return this.getGenomeHelper().getHBaseManager();
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = null;
        List<StudyConfiguration> studyConfigurationList = Collections.emptyList();
        logger.info("Get StudyConfiguration {} from DB {}", studyName, tableName);
        if (StringUtils.isEmpty(studyName)) {
            return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime),
                    studyConfigurationList.size(), studyConfigurationList.size(), "", "", studyConfigurationList);
        }
        Get get = new Get(studiesRow);
        byte[] columnQualifier = Bytes.toBytes(studyName);
        get.addColumn(genomeHelper.getColumnFamily(), columnQualifier);
        if (timeStamp != null) {
            try {
                get.setTimeRange(timeStamp + 1, Long.MAX_VALUE);
            } catch (IOException e) {
                //This should not happen ever.
                throw new IllegalArgumentException(e);
            }
        }

        try {
            if (getHBaseHelper().act(getConnection(), tableName, (table, admin) -> admin.tableExists(table.getName()))) {
                studyConfigurationList = getHBaseHelper().act(getConnection(), tableName, table -> {
                    Result result = table.get(get);
                    if (result.isEmpty()) {
                        return Collections.emptyList();
                    } else {
                        byte[] value = result.getValue(genomeHelper.getColumnFamily(), columnQualifier);
                        StudyConfiguration studyConfiguration = objectMapper.readValue(value, StudyConfiguration.class);
                        return Collections.singletonList(studyConfiguration);
                    }
                });
            }
        } catch (IOException e) {
            throw new IllegalStateException("Problem checking Table " + tableName, e);
        }
        return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime),
                studyConfigurationList.size(), studyConfigurationList.size(), "", error, studyConfigurationList);
    }

    @Override
    protected QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = "";
        logger.info("Update StudyConfiguration {}", studyConfiguration.getStudyName());
        updateStudiesSummary(studyConfiguration.getStudyName(), studyConfiguration.getStudyId(), options);
        byte[] columnQualifier = Bytes.toBytes(studyConfiguration.getStudyName());

        try {
            getHBaseHelper().act(tableName, table -> {
                byte[] bytes = objectMapper.writeValueAsBytes(studyConfiguration);
                Put put = new Put(studiesRow);
                put.addColumn(genomeHelper.getColumnFamily(), columnQualifier, studyConfiguration.getTimeStamp(), bytes);
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
        Get get = new Get(studiesRow);
        get.addColumn(genomeHelper.getColumnFamily(), studiesSummaryColumn);
        try {
            if (!getHBaseHelper().act(tableName, (table, admin) -> admin.tableExists(table.getName()))) {
                logger.info("Get StudyConfiguration summary TABLE_NO_EXISTS");
                return HashBiMap.create();
            }
            return getHBaseHelper().act(tableName, table -> {
                Result result = table.get(get);
                if (result.isEmpty()) {
                    logger.info("Get StudyConfiguration summary EMPTY");
                    return HashBiMap.create();
                } else {
                    byte[] value = result.getValue(genomeHelper.getColumnFamily(), studiesSummaryColumn);
                    Map<String, Integer> map = objectMapper.readValue(value, Map.class);
                    logger.info("Get StudyConfiguration summary {}", map);

                    return HashBiMap.create(map);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("Get StudyConfiguration summary ERROR");
            return HashBiMap.create();
        }
    }

    private void updateStudiesSummary(String study, Integer studyId, QueryOptions options) {
        BiMap<String, Integer> studiesSummary = getStudiesSummary(options);
        if (studiesSummary.getOrDefault(study, Integer.MIN_VALUE).equals(studyId)) {
            //Nothing to update
            return;
        } else {
            studiesSummary.put(study, studyId);
            updateStudiesSummary(studiesSummary, options);
        }
    }

    private void updateStudiesSummary(BiMap<String, Integer> studies, QueryOptions options) {
        try {
            VariantTableDriver.createVariantTableIfNeeded(genomeHelper, tableName, getConnection());
            try (Table table = getConnection().getTable(TableName.valueOf(tableName))) {
                byte[] bytes = objectMapper.writeValueAsBytes(studies);
                Put put = new Put(studiesRow);
                put.addColumn(genomeHelper.getColumnFamily(), studiesSummaryColumn, bytes);
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() throws IOException {
        return this.getHBaseHelper().getConnection();
    }

    public HBaseVariantStudyConfiguration toHBaseStudyConfiguration(StudyConfiguration studyConfiguration) throws IOException {
        if (studyConfiguration instanceof HBaseVariantStudyConfiguration) {
            return ((HBaseVariantStudyConfiguration) studyConfiguration);
        } else {
            List<BatchFileOperation> batches = new ArrayList<>();

            if (studyConfiguration.getAttributes() != null) {
                List<Object> batchesObj = studyConfiguration.getAttributes().getList(HBaseVariantStudyConfiguration.BATCHES_FIELD,
                        Collections.emptyList());
                for (Object o : batchesObj) {
                    batches.add(objectMapper.readValue(objectMapper.writeValueAsString(o), BatchFileOperation.class));
                }
                studyConfiguration.getAttributes().remove(HBaseVariantStudyConfiguration.BATCHES_FIELD);
            }

            return new HBaseVariantStudyConfiguration(studyConfiguration).setBatches(batches);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            this.getHBaseHelper().close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
