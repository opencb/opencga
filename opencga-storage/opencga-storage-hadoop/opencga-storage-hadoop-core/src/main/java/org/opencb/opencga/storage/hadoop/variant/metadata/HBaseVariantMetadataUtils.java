package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.io.IOException;

/**
 * Created on 07/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
class HBaseVariantMetadataUtils {

    private static final byte[] STUDIES_RK = Bytes.toBytes("studies");
    private static final byte[] PROJECT_RK = Bytes.toBytes("project");
    private static final String STUDY_CONFIGURATION_PREFIX = "SC_";
    private static final String STUDY_PREFIX = "S_";
    private static final String SAMPLE_METADATA_SEPARATOR = "_S_";
    private static final String COHORT_METADATA_SEPARATOR = "_C_";
    private static final String TASK_SEPARATOR = "_T_";
    private static final String FILE_METADATA_SEPARATOR = "_F_";
    private static final String VARIANT_FILE_METADATA_SEPARATOR = "_VF_";

    private static final String SAMPLE_NAME_INDEX_SEPARATOR = "_SIDX_";
    private static final String FILE_NAME_INDEX_SEPARATOR = "_FIDX_";
    private static final String COHORT_NAME_INDEX_SEPARATOR = "_CIDX_";
    private static final String TASK_STATUS_INDEX_SEPARATOR = "_TIDX_";

    private static final byte[] VALUE_COLUMN = Bytes.toBytes("value");
    private static final byte[] TYPE_COLUMN = Bytes.toBytes("type");
    private static final byte[] LOCK_COLUMN = Bytes.toBytes("lock");

    static final String COUNTER_PREFIX = "COUNTER_";
    static final byte[] COUNTER_PREFIX_BYTES = Bytes.toBytes(COUNTER_PREFIX);

    public enum Type {
        PROJECT,
        STUDY,
        FILE,
        SAMPLE,
        COHORT,
        TASK,
        STUDIES,
        FILES,
        INDEX,
        STUDY_CONFIGURATION,
        VARIANT_FILE_METADATA;

        private final byte[] bytes;

        Type() {
            bytes = Bytes.toBytes(name());
        }

        public byte[] bytes() {
            return bytes;
        }
    }

    public enum Status {
        READY, INVALID, DELETED, NONE;

        private final byte[] bytes;

        Status() {
            bytes = Bytes.toBytes(name());
        }

        public byte[] bytes() {
            return bytes;
        }
    }

    static byte[] getProjectRowKey() {
        return PROJECT_RK;
    }

    static byte[] getStudiesSummaryRowKey() {
        return STUDIES_RK;
    }

    static byte[] getStudyConfigurationRowKey(int studyId) {
        return Bytes.toBytes(STUDY_CONFIGURATION_PREFIX + String.valueOf(studyId));
    }

    static byte[] getStudyMetadataRowKey(int studyId) {
        return Bytes.toBytes(STUDY_PREFIX + String.valueOf(studyId));
    }

    static byte[] getSampleMetadataRowKey(int studyId, int sampleId) {
        return getStudyResourceRowKey(studyId, SAMPLE_METADATA_SEPARATOR, sampleId);
    }

    static byte[] getSampleMetadataRowKeyPrefix(int studyId) {
        return getStudyResourceRowKeyPrefix(studyId, SAMPLE_METADATA_SEPARATOR);
    }

    static byte[] getCohortMetadataRowKey(int studyId, int cohortId) {
        return getStudyResourceRowKey(studyId, COHORT_METADATA_SEPARATOR, cohortId);
    }

    static byte[] getCohortMetadataRowKeyPrefix(int studyId) {
        return getStudyResourceRowKeyPrefix(studyId, COHORT_METADATA_SEPARATOR);
    }

    static byte[] getTaskRowKey(int studyId, int taskId) {
        return getStudyResourceRowKey(studyId, TASK_SEPARATOR, taskId);
    }

    static byte[] getTaskRowKeyPrefix(int studyId) {
        return getStudyResourceRowKeyPrefix(studyId, TASK_SEPARATOR);
    }

    static byte[] getFileMetadataRowKey(int studyId, int fileId) {
        return getStudyResourceRowKey(studyId, FILE_METADATA_SEPARATOR, fileId);
    }

    static byte[] getFileMetadataRowKeyPrefix(int studyId) {
        return getStudyResourceRowKeyPrefix(studyId, FILE_METADATA_SEPARATOR);
    }

    static byte[] getVariantFileMetadataRowKey(int studyId, int fileId) {
        return getStudyResourceRowKey(studyId, VARIANT_FILE_METADATA_SEPARATOR, fileId);
    }

    static byte[] getVariantFileMetadataRowKeyPrefix(int studyId) {
        return Bytes.toBytes(STUDY_PREFIX + studyId + VARIANT_FILE_METADATA_SEPARATOR);
    }


    static byte[] getSampleNameIndexRowKey(int studyId, String sampleName) {
        return getStudyResourceRowKey(studyId, SAMPLE_NAME_INDEX_SEPARATOR, sampleName);
    }

    static byte[] getFileNameIndexRowKey(int studyId, String fileName) {
        return getStudyResourceRowKey(studyId, FILE_NAME_INDEX_SEPARATOR, fileName);
    }

    static byte[] getCohortNameIndexRowKey(int studyId, String cohortName) {
        return getStudyResourceRowKey(studyId, COHORT_NAME_INDEX_SEPARATOR, cohortName);
    }

    static byte[] getTaskStatusIndexRowKeyPrefix(int studyId, TaskMetadata.Status status) {
        return getStudyResourceRowKey(studyId, TASK_STATUS_INDEX_SEPARATOR, status.name() + "_");
    }

    static byte[] getTaskStatusIndexRowKey(int studyId, TaskMetadata.Status status, int taskId) {
        return getStudyResourceRowKey(studyId, TASK_STATUS_INDEX_SEPARATOR, status.name() + "_" + taskId);
    }

    static Pair<Integer, Integer> parseVariantFileMetadataRowKey(byte[] rk) {
        String s = Bytes.toString(rk);
        int idx = s.indexOf(VARIANT_FILE_METADATA_SEPARATOR);
        if (idx < 0) {
            throw new IllegalArgumentException("RowKey " + s + " is not a valid VariantFileMetadata RowKey!");
        }
        return Pair.of(Integer.valueOf(s.substring(STUDY_PREFIX.length(), idx)),
                Integer.valueOf(s.substring(idx + VARIANT_FILE_METADATA_SEPARATOR.length())));
    }

    static byte[] getLockColumn() {
        return LOCK_COLUMN;
    }

    static byte[] getLockColumn(String lockName) {
        return StringUtils.isEmpty(lockName) ? getLockColumn() : Bytes.toBytes(lockName);
    }

    static byte[] getTypeColumn() {
        return TYPE_COLUMN;
    }

    static byte[] getValueColumn() {
        return VALUE_COLUMN;
    }

    static byte[] getCounterColumn(Integer studyId, String idType) {
        String id = COUNTER_PREFIX + idType + (studyId == null ? "" : ("_" + studyId));
        return Bytes.toBytes(id);
    }

    static boolean createMetaTableIfNeeded(HBaseManager hBaseManager, String tableName, byte[] columnFamily) throws IOException {
        return hBaseManager.createTableIfNeeded(tableName, columnFamily, Compression.Algorithm.NONE);
    }

    private static byte[] getStudyResourceRowKeyPrefix(int studyId, String separator) {
        return Bytes.toBytes(STUDY_PREFIX + studyId + separator);
    }

    private static byte[] getStudyResourceRowKey(int studyId, String separator, int id) {
        return Bytes.toBytes(STUDY_PREFIX + studyId + separator + StringUtils.leftPad(String.valueOf(id), 10, '0'));
    }

    private static byte[] getStudyResourceRowKey(int studyId, String separator, String name) {
        return Bytes.toBytes(STUDY_PREFIX + studyId + separator + name);
    }
}
