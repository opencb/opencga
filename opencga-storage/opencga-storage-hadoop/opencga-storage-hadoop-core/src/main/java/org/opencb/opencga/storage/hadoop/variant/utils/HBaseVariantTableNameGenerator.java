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
    private static final String ARCHIVE_SUFIX = "_archive_";

    private final String namespace;
    private final String dbName;
    private final String variantTableName;


    public HBaseVariantTableNameGenerator(String dbName, ObjectMap options) {
        this.dbName = dbName;
        namespace = options.getString(HadoopVariantStorageEngine.HBASE_NAMESPACE, "");
        variantTableName = getVariantTableName(this.dbName, options);
    }

    public HBaseVariantTableNameGenerator(String dbName, Configuration conf) {
        this.dbName = dbName;
        namespace = conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, "");
        variantTableName = getVariantTableName(this.dbName, conf);
    }

    public String getVariantTableName() {
        return variantTableName;
    }

    public String getArchiveTableName(int studyId) {
        return getArchiveTableName(dbName, studyId, namespace);
    }

    @Deprecated
    public static String getArchiveTableName(Object a, Object b) {
        return "";
    }

    public static String getDBNameFromArchiveTableName(String archiveTableName) {
        int endIndex = archiveTableName.lastIndexOf(archiveTableName);
        if (endIndex > 0 && StringUtils.isNumeric(archiveTableName.substring(endIndex + ARCHIVE_SUFIX.length()))) {
            return archiveTableName.substring(0, endIndex);
        } else {
            throw new IllegalArgumentException("Invalid archive table name : " + archiveTableName);
        }
    }

    public static String getDBNameFromVariantsTableName(String variantsTableName) {
        if (variantsTableName.endsWith(VARIANTS_SUFIX)) {
            return variantsTableName.substring(0, variantsTableName.length() - VARIANTS_SUFIX.length());
        } else {
            throw new IllegalArgumentException("Invalid variants table name : " + variantsTableName);
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
        return getArchiveTableName(dbName, studyId, conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""));
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
        return getArchiveTableName(dbName, studyId, conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""));
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
        return getArchiveTableName(dbName, studyId, options.getString(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""));
    }

    /**
     * Get the archive table name given a StudyId.
     *
     * @param dbName given database name
     * @param studyId Numerical study identifier
     * @param namespace hbase namespace
     * @return Table name
     */
    public static String getArchiveTableName(String dbName, int studyId, String namespace) {
        return buildTableName(namespace, dbName, ARCHIVE_SUFIX + studyId);
    }

    public static String getVariantTableName(String dbName, ObjectMap options) {
        return buildTableName(options.getString(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), "", dbName + VARIANTS_SUFIX);
    }

    public static String getVariantTableName(String dbName, Configuration conf) {
        return buildTableName(conf.get(HadoopVariantStorageEngine.HBASE_NAMESPACE, ""), "", dbName + VARIANTS_SUFIX);
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
            sb.append(namespace).append(":");
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
