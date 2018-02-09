package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;

/**
 * Created on 07/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
class HBaseVariantMetadataUtils {

    private static final byte[] STUDIES_RK = Bytes.toBytes("studies");
    private static final String FILES_SUMMARY_SUFIX_RK = "_files";

    private static final byte[] VALUE_COLUMN = Bytes.toBytes("value");
    private static final byte[] TYPE_COLUMN = Bytes.toBytes("type");
    private static final byte[] LOCK_COLUMN = Bytes.toBytes("lock");

    public enum Type {
        STUDY_CONFIGURATION, STUDIES, VARIANT_FILE_METADATA, FILES;

        private final byte[] bytes;

        Type() {
            bytes = Bytes.toBytes(name());
        }

        public byte[] bytes() {
            return bytes;
        }
    }

    static byte[] getStudiesSummaryRowKey() {
        return STUDIES_RK;
    }

    static byte[] getStudyConfigurationRowKey(StudyConfiguration studyConfiguration) {
        return getStudyConfigurationRowKey(studyConfiguration.getStudyId());
    }

    static byte[] getStudyConfigurationRowKey(int studyId) {
        return Bytes.toBytes(String.valueOf(studyId));
    }

    static byte[] getVariantFileMetadataRowKey(int studyId, int fileId) {
        return Bytes.toBytes(studyId + "_" + fileId);
    }

    static byte[] getVariantFileMetadataRowKeyPrefix(int studyId) {
        return Bytes.toBytes(studyId + "_");
    }

    static byte[] getFilesSummaryRowKey(int studyId) {
        return Bytes.toBytes(studyId + FILES_SUMMARY_SUFIX_RK);
    }

    static Pair<Integer, Integer> parseVariantFileMetadataRowKey(byte[] rk) {
        String s = Bytes.toString(rk);
        String[] split = s.split("_");
        if (split.length != 2) {
            throw new IllegalArgumentException("RowKey " + s + " is not a valid VariantFileMetadata RowKey!");
        }
        return Pair.of(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
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

    static boolean createMetaTableIfNeeded(HBaseManager hBaseManager, String tableName, GenomeHelper genomeHelper) throws IOException {
        return hBaseManager.createTableIfNeeded(tableName, genomeHelper.getColumnFamily(), Compression.Algorithm.NONE);
    }

}
