package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.utils.HBaseLockManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.GenomeHelper.PHOENIX_INDEX_LOCK_COLUMN;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.*;

public class VariantPhoenixSchemaManager {

    private static final String PENDING_PHOENIX_COLUMNS = "pending_phoenix_columns";
    private static final long MAX_LOCK_TIMEOUT = TimeUnit.MINUTES.toMillis(30);
    private static final long LOCK_ATTEMPT_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
    private static final long MAX_ATTEMPTS = MAX_LOCK_TIMEOUT / LOCK_ATTEMPT_TIMEOUT;

    private final Connection con;
    private final String variantsTableName;
    private final PhoenixHelper phoenixHelper;
    private final VariantStorageMetadataManager metadataManager;
    private final String metaTableName;
    private Set<String> _existingColumns;

    protected static Logger logger = LoggerFactory.getLogger(VariantPhoenixSchemaManager.class);

    public VariantPhoenixSchemaManager(VariantHadoopDBAdaptor dbAdaptor) {
        this(dbAdaptor.getConfiguration(), dbAdaptor.getVariantTable(), dbAdaptor.getMetadataManager(), dbAdaptor.getJdbcConnection());
    }

    public VariantPhoenixSchemaManager(Configuration conf, String variantsTableName, VariantStorageMetadataManager metadataManager)
            throws SQLException, ClassNotFoundException {
        this(conf, variantsTableName, metadataManager, new PhoenixHelper(conf).newJdbcConnection());
    }

    public VariantPhoenixSchemaManager(Configuration conf, String variantsTableName, VariantStorageMetadataManager metadataManager,
                                       Connection con) {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        this.con = con;
        this.metadataManager = metadataManager;
        phoenixHelper = new PhoenixHelper(conf);
        this.variantsTableName = variantsTableName;
        String dbName = HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName(variantsTableName);
        this.metaTableName = HBaseVariantTableNameGenerator.getMetaTableName(null, dbName);
    }

    public void registerPendingColumns() throws StorageEngineException {
        Set<PhoenixHelper.Column> pendingColumns = getPendingColumns(metadataManager.getProjectMetadata());
        if (!pendingColumns.isEmpty()) {
            registerColumns(pendingColumns);
        }
    }

    public void registerAnnotationColumns() throws StorageEngineException {
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantsTableName);
        Collection<PhoenixHelper.Column> columns = Arrays.stream(VariantColumn.values()).collect(Collectors.toList());
        ProjectMetadata projectMetadata = metadataManager.getProjectMetadata();

        //TODO: Read population frequencies columns from ProjectMetadata ?
        if (projectMetadata.getSpecies().equals("hsapiens")) {
            columns.addAll(getHumanPopulationFrequenciesColumns());
        }
        columns.addAll(getReleaseColumns(projectMetadata.getRelease()));

        columns = updatePendingColumns(columns);
        if (!columns.isEmpty()) {
            registerColumns(columns);
        }
    }

    /**
     * Register new files as phoenix columns. It includes sample columns and other auxiliar columns.
     *
     * @param studyId Study id
     * @param fileIds List of file ids.
     * @throws StorageEngineException on errors.
     */
    public void registerNewFiles(int studyId, List<Integer> fileIds) throws StorageEngineException {
        List<PhoenixHelper.Column> columns = new LinkedList<>();
        ProjectMetadata projectMetadata = metadataManager.getProjectMetadata();

        columns.addAll(getStudyColumns(studyId));
        columns.addAll(buildNewFilesAndSamplesColumns(studyId, fileIds));
        columns.addAll(getReleaseColumns(projectMetadata.getRelease()));
        if (projectMetadata.getSpecies().equals("hsapiens")) {
            columns.addAll(getHumanPopulationFrequenciesColumns());
        }

        registerColumns(columns);
    }

    public void dropFiles(int studyId, List<Integer> fileIds) throws SQLException {
        List<Integer> sampleIds = new ArrayList<>();
        for (Integer fileId : fileIds) {
            sampleIds.addAll(metadataManager.getFileMetadata(studyId, fileId).getSamples());
        }
        List<CharSequence> columns = new ArrayList<>(fileIds.size() + sampleIds.size());
        for (Integer fileId : fileIds) {
            columns.add(buildFileColumnKey(studyId, fileId, new StringBuilder()));
        }
        for (Integer sampleId : sampleIds) {
            columns.add(buildSampleColumnKey(studyId, sampleId, new StringBuilder()));
        }
        phoenixHelper.dropColumns(con, variantsTableName, columns, DEFAULT_TABLE_TYPE);
        con.commit();
    }

    public void registerNewScore(int studyId, int scoreId) throws StorageEngineException {
        PhoenixHelper.Column column = getVariantScoreColumn(studyId, scoreId);
        registerColumns(Collections.singletonList(column));
    }

    public void dropScore(int studyId, List<Integer> scores) throws SQLException {
        List<CharSequence> columns = new ArrayList<>(scores.size());
        for (Integer score : scores) {
            columns.add(getVariantScoreColumn(studyId, score).column());
        }
        phoenixHelper.dropColumns(con, variantsTableName, columns, DEFAULT_TABLE_TYPE);
        con.commit();
    }

    public void registerNewCohorts(int studyId, List<Integer> cohortIds) throws StorageEngineException {
        List<PhoenixHelper.Column> columns = new ArrayList<>();
        for (Integer cohortId : cohortIds) {
            columns.addAll(getStatsColumns(studyId, cohortId));
        }

        registerColumns(columns);
    }

    private List<PhoenixHelper.Column> buildNewFilesAndSamplesColumns(Integer studyId, Collection<Integer> fileIds) {
        List<PhoenixHelper.Column> columns = new LinkedList<>();
        Set<Integer> newSamples = new HashSet<>();

        for (Integer fileId : fileIds) {
            FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, fileId);
            newSamples.addAll(fileMetadata.getSamples());
            columns.add(getFileColumn(studyId, fileId));
        }
        for (Integer sampleId : newSamples) {
            columns.add(getSampleColumn(studyId, sampleId));
        }
        for (Integer sampleId : newSamples) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
            if (VariantStorageEngine.SplitData.MULTI.equals(sampleMetadata.getSplitData())) {
                // If multi file load, register all secondary sample columns
                for (Integer fileId : sampleMetadata.getFiles().subList(1, sampleMetadata.getFiles().size())) {
                    columns.add(getSampleColumn(studyId, sampleId, fileId));
                }
            }
        }
        return columns;
    }

    /**
     * Register list of columns with "ALTER VIEW ... ADD .... IF NOT EXISTS" .
     * This process locks the table to avoid concurrent table schema alter.
     * New columns to be added are written into {@link ProjectMetadata#getAttributes()} as {@link #PENDING_PHOENIX_COLUMNS}.
     * All pending columns will be registered once the lock is obtained.
     *
     * @param columns List of columns to add
     * @throws StorageEngineException on errors
     */
    private void registerColumns(Collection<PhoenixHelper.Column> columns) throws StorageEngineException {
        try {
            if (!phoenixHelper.tableExists(con, variantsTableName)) {
                try (Lock lock = lockTable()) {
                    String namespace = SchemaUtil.getSchemaNameFromFullName(variantsTableName);
                    if (StringUtils.isNotEmpty(namespace)) {
//                    HBaseManager.createNamespaceIfNeeded(con, namespace);
                        createSchemaIfNeeded(namespace);
                    }
                    createTableIfNeeded();
                }
            }
        } catch (SQLException e) {
            throw new StorageEngineException("Error creating table", e);
        }

        Lock lock = null;
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            int attempt = 0;
            Set<PhoenixHelper.Column> pendingColumns;
            do {
                pendingColumns = updatePendingColumns(columns);
                if (pendingColumns.isEmpty()) {
                    // No columns to add. Stop trying to get the lock!
                    break;
                }
                lock = tryLockTable(attempt);
                attempt++;
            } while (lock == null);

            // Update pending columns one last time, after getting the lock.
            if (!pendingColumns.isEmpty()) {
                pendingColumns = updatePendingColumns(columns);
            }

            String msg;
            if (pendingColumns.isEmpty()) {
                msg = "Columns already in phoenix. Nothing to do! Had to wait " + TimeUtils.durationToString(stopWatch);
            } else {
                phoenixHelper
                        .addMissingColumns(con, variantsTableName, pendingColumns,
                                DEFAULT_TABLE_TYPE, getExistingColumns());
                // Final update to remove new added columns
                updatePendingColumns(Collections.emptyList());
                msg = "Added new columns to Phoenix in " + TimeUtils.durationToString(stopWatch);
            }
            if (stopWatch.getTime(TimeUnit.SECONDS) < 10) {
                logger.info(msg);
            } else {
                logger.warn("Slow phoenix response");
                logger.warn(msg);
            }

        } catch (SQLException | StorageEngineException e) {
            throw new StorageEngineException("Error locking table to modify Phoenix columns!", e);
        } finally {
            try {
                if (lock != null) {
//                    logger.info("Release lock " + lock);
                    lock.unlock();
                }
            } catch (HBaseLockManager.IllegalLockStatusException e) {
                logger.warn(e.getMessage());
                logger.debug(e.getMessage(), e);
            }
        }
    }

    private void createSchemaIfNeeded(String schema) throws SQLException {
        String sql = "CREATE SCHEMA IF NOT EXISTS \"" + schema + "\"";
        logger.debug(sql);
        try {
            phoenixHelper.execute(con, sql);
        } catch (SQLException e) {
            if (e.getCause() != null && e.getCause() instanceof NamespaceExistException) {
                logger.debug("Namespace already exists", e);
            } else {
                throw e;
            }
        }
    }

    private void createTableIfNeeded() throws SQLException {
        if (!phoenixHelper.tableExists(con, variantsTableName)) {
            String sql = buildCreate(variantsTableName);
            logger.info(sql);
            try {
                phoenixHelper.execute(con, sql);
            } catch (Exception e) {
                if (!phoenixHelper.tableExists(con, variantsTableName)) {
                    throw e;
                } else {
                    logger.info(DEFAULT_TABLE_TYPE + " {} already exists", variantsTableName);
                    logger.debug(DEFAULT_TABLE_TYPE + " " + variantsTableName + " already exists. Hide exception", e);
                }
            }
        } else {
            logger.debug(DEFAULT_TABLE_TYPE + " {} already exists", variantsTableName);
        }
    }

    private String buildCreate(String variantsTableName) {
        PTableType tableType = DEFAULT_TABLE_TYPE;
        StringBuilder sb = new StringBuilder().append("CREATE ").append(tableType).append(" IF NOT EXISTS ")
                .append(phoenixHelper.getEscapedFullTableName(tableType, variantsTableName)).append(' ').append('(');
        for (VariantColumn variantColumn : VariantColumn.values()) {
            switch (variantColumn) {
                case CHROMOSOME:
                case POSITION:
                    sb.append(" \"").append(variantColumn).append("\" ").append(variantColumn.sqlType()).append(" NOT NULL , ");
                    break;
                default:
                    sb.append(" \"").append(variantColumn).append("\" ").append(variantColumn.sqlType()).append(" , ");
                    break;
            }
        }

//        for (Column column : VariantPhoenixHelper.HUMAN_POPULATION_FREQUENCIES_COLUMNS) {
//            sb.append(" \"").append(column).append("\" ").append(column.sqlType()).append(" , ");
//        }

        sb.append(" CONSTRAINT PK PRIMARY KEY (");
        for (Iterator<PhoenixHelper.Column> iterator = PRIMARY_KEY.iterator(); iterator.hasNext();) {
            PhoenixHelper.Column column = iterator.next();
            sb.append(column);
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.append(") )").toString();
    }

    private Set<String> getExistingColumns() throws StorageEngineException {
        return getExistingColumns(false);
    }

    private Set<String> getExistingColumns(boolean update) throws StorageEngineException {
        if (_existingColumns == null || update) {
            try {
                _existingColumns = phoenixHelper
                        .getColumns(con, variantsTableName, DEFAULT_TABLE_TYPE).stream()
                        .map(PhoenixHelper.Column::column)
                        .collect(Collectors.toSet());
            } catch (SQLException e) {
                throw new StorageEngineException("Problem reading existing columns", e);
            }
        }
        return _existingColumns;
    }

    private Set<PhoenixHelper.Column> updatePendingColumns(Collection<PhoenixHelper.Column> columns)
            throws StorageEngineException {
        Set<String> existingColumns = getExistingColumns(true);

        ProjectMetadata projectMetadata = metadataManager.updateProjectMetadata(pm -> {
            // Merge pending columns with new columns
            Set<PhoenixHelper.Column> pendingColumns = getPendingColumns(pm);
            pendingColumns.addAll(columns);
            // Remove existing columns from pending list
            pendingColumns = pendingColumns.stream()
                    .filter(column -> !existingColumns.contains(column.column()))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            setPendingColumns(pm, pendingColumns);
            return pm;
        });
        return getPendingColumns(projectMetadata);
    }

    private Set<PhoenixHelper.Column> getPendingColumns(ProjectMetadata projectMetadata) {
        List<String> pendingPhoenixColumnsStr = projectMetadata.getAttributes().getAsStringList(PENDING_PHOENIX_COLUMNS);
        Set<PhoenixHelper.Column> pendingPhoenixColumns = new LinkedHashSet<>(pendingPhoenixColumnsStr.size() / 2);
        for (int i = 0; i < pendingPhoenixColumnsStr.size(); i += 2) {
            String name = pendingPhoenixColumnsStr.get(i);
            String sqlType = pendingPhoenixColumnsStr.get(i + 1);
            pendingPhoenixColumns.add(PhoenixHelper.Column.build(name, PDataType.fromSqlTypeName(sqlType)));
        }
        return pendingPhoenixColumns;
    }

    private ProjectMetadata setPendingColumns(ProjectMetadata projectMetadata, Set<PhoenixHelper.Column> pendingColumns) {
        List<String> pendingPhoenixColumnsStr = new ArrayList<>(pendingColumns.size() * 2);
        for (PhoenixHelper.Column pendingColumn : pendingColumns) {
            pendingPhoenixColumnsStr.add(pendingColumn.column());
            pendingPhoenixColumnsStr.add(pendingColumn.sqlType());
        }
        projectMetadata.getAttributes().put(PENDING_PHOENIX_COLUMNS, pendingPhoenixColumnsStr);
        return projectMetadata;
    }

    private Lock tryLockTable(int attempt) throws StorageEngineException {
        long lockDuration = TimeUnit.MINUTES.toMillis(5);
        long timeout;
        if (attempt == 0) {
            timeout = TimeUnit.SECONDS.toMillis(5);
        } else {
            timeout = LOCK_ATTEMPT_TIMEOUT;
            if (attempt == 1) {
                logger.info("Waiting to get Lock over HBase table {} up to {} minutes ...", metaTableName, MAX_LOCK_TIMEOUT);
            }
            logger.info("Lock attempt {}/{}, timeout of {}s", attempt, MAX_ATTEMPTS, TimeUnit.MILLISECONDS.toSeconds(LOCK_ATTEMPT_TIMEOUT));
        }
        try {
            Lock lock = metadataManager.lockGlobal(lockDuration, timeout, GenomeHelper.PHOENIX_LOCK_COLUMN);
//            logger.info("Gain lock " + lock);
            return lock;
        } catch (StorageEngineException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                if (attempt < MAX_ATTEMPTS) {
                    return null;
                }
            }
            throw e;
        }
    }

    private Lock lockTable() throws StorageEngineException {
        Lock lock = null;
        long lockDuration = TimeUnit.MINUTES.toMillis(5);
        try {
            lock = metadataManager.lockGlobal(lockDuration,
                    TimeUnit.SECONDS.toMillis(5), GenomeHelper.PHOENIX_LOCK_COLUMN);
        } catch (StorageEngineException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                int timeout = 30;
                logger.info("Waiting to get Lock over HBase table {} up to {} minutes ...", metaTableName, timeout);
                lock = metadataManager.lockGlobal(lockDuration,
                        TimeUnit.MINUTES.toMillis(timeout), GenomeHelper.PHOENIX_LOCK_COLUMN);
            } else {
                throw e;
            }
        }
        return lock;
    }

    public void createPhoenixIndexes() throws StorageEngineException {
        if (DEFAULT_TABLE_TYPE == PTableType.VIEW) {
            logger.debug("Skip create indexes for VIEW table");
        } else {
            final String species = metadataManager.getProjectMetadata().getSpecies();
            Lock lock = null;
            try {
                lock = metadataManager.lockGlobal(TimeUnit.MINUTES.toMillis(60),
                        TimeUnit.SECONDS.toMillis(5), PHOENIX_INDEX_LOCK_COLUMN);
                if (species.equals("hsapiens")) {
                    List<PhoenixHelper.Index> popFreqIndices = getPopFreqIndices(variantsTableName);
                    phoenixHelper.createIndexes(con, DEFAULT_TABLE_TYPE,
                            variantsTableName, popFreqIndices, false);
                }
                List<PhoenixHelper.Index> indices = getIndices(variantsTableName);
                phoenixHelper.createIndexes(con, DEFAULT_TABLE_TYPE, variantsTableName, indices, false);
            } catch (SQLException e) {
                throw new StorageEngineException("Unable to create Phoenix Indexes", e);
            } catch (StorageEngineException e) {
                if (e.getCause() instanceof TimeoutException) {
                    // Indices are been created by another instance. Don't need to create twice.
                    logger.info("Unable to get lock to create PHOENIX INDICES. Already been created by another instance. "
                            + "Skip create indexes!");
                } else {
                    throw new StorageEngineException("Unable to create Phoenix Indexes", e);
                }
            } finally {
                if (lock != null) {
                    lock.unlock();
                }
            }
        }
    }

}
