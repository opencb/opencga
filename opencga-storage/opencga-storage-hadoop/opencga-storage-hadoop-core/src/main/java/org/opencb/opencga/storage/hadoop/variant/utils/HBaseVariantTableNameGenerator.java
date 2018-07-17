package org.opencb.opencga.storage.hadoop.variant.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;

/**
 * Created on 02/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantTableNameGenerator {

    private static final String VARIANTS_SUFIX = "_variants";
    private static final String META_SUFIX = "_meta";
    private static final String ARCHIVE_SUFIX = "_archive_";
    private static final String SAMPLE_SUFIX = "_gt_";
    private static final int MINIMUM_DB_NAME_SIZE = 1;

    private final String namespace;
    private final String dbName;
    private final String variantTableName;
    private final String metaTableName;


    public HBaseVariantTableNameGenerator(String dbName, ObjectMap options) {
        if (dbName.length() < MINIMUM_DB_NAME_SIZE) {
            throw new IllegalArgumentException("dbName size must be more than 1!");
        }
        this.dbName = dbName;
        namespace = options.getString(HadoopVariantStorageEngine.HBASE_NAMESPACE, "");
        variantTableName = getVariantTableName(this.dbName, options);
        metaTableName = getMetaTableName(this.dbName, options);
    }

    public HBaseVariantTableNameGenerator(String dbName, Configuration conf) {
        if (dbName.length() < MINIMUM_DB_NAME_SIZE) {
            throw new IllegalArgumentException("dbName size must be more than 1!");
        }
        this.dbName = dbName;
        namespace = conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, "");
        variantTableName = getVariantTableName(this.dbName, conf);
        metaTableName = getMetaTableName(this.dbName, conf);
    }

    public String getVariantTableName() {
        return variantTableName;
    }

    public String getArchiveTableName(int studyId) {
        return getArchiveTableName(namespace, dbName, studyId);
    }

    public String getSampleIndexTableName(int studyId) {
        return getSampleIndexTableName(namespace, dbName, studyId);
    }

    public String getMetaTableName() {
        return metaTableName;
    }

    public static String getDBNameFromArchiveTableName(String archiveTableName) {
        int endIndex = checkValidArchiveTableNameGetEndIndex(archiveTableName);
        return archiveTableName.substring(0, endIndex);
    }

    public static void checkValidArchiveTableName(String archiveTableName) {
        checkValidArchiveTableNameGetEndIndex(archiveTableName);
    }

    private static int checkValidArchiveTableNameGetEndIndex(String archiveTableName) {
        int endIndex = archiveTableName.lastIndexOf(archiveTableName);
        if (endIndex <= 0 || !StringUtils.isNumeric(archiveTableName.substring(endIndex + ARCHIVE_SUFIX.length()))) {
            throw new IllegalArgumentException("Invalid archive table name : " + archiveTableName);
        }
        return endIndex;
    }

    public static String getDBNameFromVariantsTableName(String variantsTableName) {
        checkValidVariantsTableName(variantsTableName);
        return variantsTableName.substring(0, variantsTableName.length() - VARIANTS_SUFIX.length());
    }

    public static void checkValidVariantsTableName(String variantsTableName) {
        if (!variantsTableName.endsWith(VARIANTS_SUFIX) || variantsTableName.length() <= VARIANTS_SUFIX.length() + MINIMUM_DB_NAME_SIZE) {
            throw new IllegalArgumentException("Invalid variants table name : " + variantsTableName);
        }
    }

    public static String getDBNameFromMetaTableName(String metaTableName) {
        checkValidMetaTableName(metaTableName);
        return metaTableName.substring(0, metaTableName.length() - META_SUFIX.length());
    }

    public static void checkValidMetaTableName(String metaTableName) {
        if (!metaTableName.endsWith(META_SUFIX) || metaTableName.length() <= META_SUFIX.length() + MINIMUM_DB_NAME_SIZE) {
            throw new IllegalArgumentException("Invalid meta table name : " + metaTableName);
        }
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param variantsTableName Variant table name
     * @param studyId Numerical study identifier
     * @param conf Hadoop configuration with the OpenCGA values.
     * @return Table name
     */
    public static String getArchiveTableNameFromVariantsTable(String variantsTableName, int studyId, Configuration conf) {
        String dbName = getDBNameFromVariantsTableName(variantsTableName);
        return getArchiveTableName(conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), dbName, studyId);
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param dbName given database name
     * @param studyId Numerical study identifier
     * @param conf Hadoop configuration with the OpenCGA values.
     * @return Table name
     */
    public static String getArchiveTableName(String dbName, int studyId, Configuration conf) {
        return getArchiveTableName(conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), dbName, studyId);
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param dbName given database name
     * @param studyId Numerical study identifier
     * @param options Options
     * @return Table name
     */
    public static String getArchiveTableName(String dbName, int studyId, ObjectMap options) {
        return getArchiveTableName(options.getString(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), dbName, studyId);
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param namespace hbase namespace
     * @param dbName given database name
     * @param studyId Numerical study identifier
     * @return Table name
     */
    public static String getArchiveTableName(String namespace, String dbName, int studyId) {
        if (studyId <= 0) {
            throw new IllegalArgumentException("Can not get archive table name. Invalid studyId!");
        }
        return buildTableName(namespace, "", dbName + ARCHIVE_SUFIX + studyId);
    }

    public static int getStudyIdFromArchiveTable(String archiveTable) {
        int idx = archiveTable.lastIndexOf(ARCHIVE_SUFIX.charAt(ARCHIVE_SUFIX.length() - 1));
        return Integer.valueOf(archiveTable.substring(idx + 1));
    }

    public static String getSampleIndexTableName(String namespace, String dbName, int studyId) {
        return buildTableName(namespace, "", dbName + SAMPLE_SUFIX + studyId);
    }

    public static String getVariantTableName(String dbName, ObjectMap options) {
        return buildTableName(options.getString(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), "", dbName + VARIANTS_SUFIX);
    }

    public static String getVariantTableName(String dbName, Configuration conf) {
        return buildTableName(conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), "", dbName + VARIANTS_SUFIX);
    }

    public static String getMetaTableName(String dbName, ObjectMap options) {
        return buildTableName(options.getString(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), "", dbName + META_SUFIX);
    }

    public static String getMetaTableName(String dbName, Configuration conf) {
        return buildTableName(conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), "", dbName + META_SUFIX);
    }

    protected static String buildTableName(String namespace, String prefix, String tableName) {
        StringBuilder sb = new StringBuilder();

        if (StringUtils.isNotEmpty(namespace)) {
            if (tableName.contains(":")) {
                if (!tableName.startsWith(namespace + ":")) {
                    throw new IllegalArgumentException("Wrong namespace : '" + tableName + "'."
                            + " Namespace mismatches with the read from configuration:" + namespace);
                } else {
                    tableName = tableName.substring(tableName.indexOf(':') + 1); // Remove '<namespace>:'
                }
            }
            sb.append(namespace).append(':');
        }
        if (StringUtils.isNotEmpty(prefix)) {
            if (prefix.endsWith("_") && tableName.startsWith("_")) {
                sb.append(prefix, 0, prefix.length() - 1);
            } else {
                sb.append(prefix);
            }
            if (!prefix.endsWith("_") && !tableName.startsWith("_")) {
                sb.append("_");
            }
        }
        sb.append(tableName);

        String fullyQualified = sb.toString();
        TableName.isLegalFullyQualifiedTableName(fullyQualified.getBytes());
        return fullyQualified;
    }

}
