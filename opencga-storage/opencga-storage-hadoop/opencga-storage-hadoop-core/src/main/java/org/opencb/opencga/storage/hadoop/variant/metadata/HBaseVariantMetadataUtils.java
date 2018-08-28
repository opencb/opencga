package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
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
    private static final String STUDY_PREFIX = "S_";
    private static final String FILE_METADATA_SEPARATOR = "_F_";

    private static final byte[] VALUE_COLUMN = Bytes.toBytes("value");
    private static final byte[] TYPE_COLUMN = Bytes.toBytes("type");
    private static final byte[] LOCK_COLUMN = Bytes.toBytes("lock");
    private static final byte[] STATUS_COLUMN = Bytes.toBytes("status");

    static final String COUNTER_PREFIX = "COUNTER_";
    static final byte[] COUNTER_PREFIX_BYTES = Bytes.toBytes(COUNTER_PREFIX);

    public enum Type {
        PROJECT, STUDY_CONFIGURATION, STUDIES, VARIANT_FILE_METADATA, FILES;

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

    static byte[] getStudyConfigurationRowKey(StudyConfiguration studyConfiguration) {
        return getStudyConfigurationRowKey(studyConfiguration.getStudyId());
    }

    static byte[] getStudyConfigurationRowKey(int studyId) {
        return Bytes.toBytes(STUDY_PREFIX + String.valueOf(studyId));
    }

    static byte[] getVariantFileMetadataRowKey(int studyId, int fileId) {
        return Bytes.toBytes(STUDY_PREFIX + studyId + FILE_METADATA_SEPARATOR + fileId);
    }

    static byte[] getVariantFileMetadataRowKeyPrefix(int studyId) {
        return Bytes.toBytes(STUDY_PREFIX + studyId + FILE_METADATA_SEPARATOR);
    }

    static Pair<Integer, Integer> parseVariantFileMetadataRowKey(byte[] rk) {
        String s = Bytes.toString(rk);
        int idx = s.indexOf(FILE_METADATA_SEPARATOR);
        if (idx < 0) {
            throw new IllegalArgumentException("RowKey " + s + " is not a valid VariantFileMetadata RowKey!");
        }
        return Pair.of(Integer.valueOf(s.substring(STUDY_PREFIX.length(), idx)),
                Integer.valueOf(s.substring(idx + FILE_METADATA_SEPARATOR.length())));
    }

    static byte[] getLockColumn() {
        return LOCK_COLUMN;
    }

    static byte[] getTypeColumn() {
        return TYPE_COLUMN;
    }

    static byte[] getValueColumn() {
        return VALUE_COLUMN;
    }

    static byte[] getStatusColumn() {
        return STATUS_COLUMN;
    }

    static byte[] getCounterColumn(StudyConfiguration studyConfiguration, String idType) {
        String id = COUNTER_PREFIX + idType + (studyConfiguration == null ? "" : ("_" + studyConfiguration.getStudyId()));
        return Bytes.toBytes(id);
    }

    static boolean createMetaTableIfNeeded(HBaseManager hBaseManager, String tableName, byte[] columnFamily) throws IOException {
        return hBaseManager.createTableIfNeeded(tableName, columnFamily, Compression.Algorithm.NONE);
    }

}
