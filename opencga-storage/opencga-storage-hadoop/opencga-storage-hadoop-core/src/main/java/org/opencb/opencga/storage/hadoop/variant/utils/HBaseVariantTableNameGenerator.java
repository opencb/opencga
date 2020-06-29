package org.opencb.opencga.storage.hadoop.variant.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;

/**
 * Created on 02/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantTableNameGenerator {

    private static final String VARIANTS_SUFIX = "_variants";
    private static final String META_SUFIX = "_meta";
    private static final String ARCHIVE_SUFIX = "_archive_";
    private static final String SAMPLE_SUFIX = "_variant_sample_index_";
    private static final String ANNOTATION_SUFIX = "_annotation";
    private static final String PENDING_ANNOTATION_SUFIX = "_pending_annotation";
    private static final String PENDING_SECONDARY_INDEX_SUFIX = "_pending_secondary_index";
    private static final int MINIMUM_DB_NAME_SIZE = 1;

    private final String namespace;
    private final String dbName;
    private final String variantTableName;
    private final String metaTableName;
    private final String annotationIndexTableName;
    private final String pendingAnnotationTableName;
    private final String pendingSecondaryIndexTableName;


    public HBaseVariantTableNameGenerator(String dbName, ObjectMap options) {
        this(options.getString(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), ""), dbName);
    }

    public HBaseVariantTableNameGenerator(String dbName, Configuration conf) {
        this(conf.get(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), ""), dbName);
    }

    public HBaseVariantTableNameGenerator(String namespace, String dbName) {
        if (dbName.length() < MINIMUM_DB_NAME_SIZE) {
            throw new IllegalArgumentException("dbName size must be more than 1!");
        }
        this.namespace = namespace;
        this.dbName = dbName;
        variantTableName = getVariantTableName(namespace, this.dbName);
        metaTableName = getMetaTableName(namespace, this.dbName);
        annotationIndexTableName = getAnnotationIndexTableName(namespace, this.dbName);
        pendingAnnotationTableName = getPendingAnnotationTableName(namespace, this.dbName);
        pendingSecondaryIndexTableName = getPendingSecondaryIndexTableName(namespace, this.dbName);
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

    public String getAnnotationIndexTableName() {
        return annotationIndexTableName;
    }

    public String getPendingAnnotationTableName() {
        return pendingAnnotationTableName;
    }

    public String getPendingSecondaryIndexTableName() {
        return pendingSecondaryIndexTableName;
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
        if (!validSuffix(variantsTableName, VARIANTS_SUFIX)) {
            throw new IllegalArgumentException("Invalid variants table name : " + variantsTableName);
        }
    }

    public static void checkValidPendingAnnotationTableName(String pendingAnnotationTableName) {
        if (!validSuffix(pendingAnnotationTableName, PENDING_ANNOTATION_SUFIX)) {
            throw new IllegalArgumentException("Invalid pending annotation table name : " + pendingAnnotationTableName);
        }
    }

    public static void checkValidPendingSecondaryIndexTableName(String pendingSecondaryIndexTableName) {
        if (!validSuffix(pendingSecondaryIndexTableName, PENDING_SECONDARY_INDEX_SUFIX)) {
            throw new IllegalArgumentException("Invalid pending secondary index table name : " + pendingSecondaryIndexTableName);
        }
    }

    public static String getDBNameFromMetaTableName(String metaTableName) {
        checkValidMetaTableName(metaTableName);
        return metaTableName.substring(0, metaTableName.length() - META_SUFIX.length());
    }

    public static void checkValidMetaTableName(String metaTableName) {
        if (!validSuffix(metaTableName, META_SUFIX)) {
            throw new IllegalArgumentException("Invalid meta table name : " + metaTableName);
        }
    }

    private static boolean validSuffix(String tableName, String suffix) {
        return tableName.endsWith(suffix) && tableName.length() > suffix.length() + MINIMUM_DB_NAME_SIZE;
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
        return getArchiveTableName(conf.get(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), ""), dbName, studyId);
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
        return getArchiveTableName(conf.get(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), ""), dbName, studyId);
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
        return getArchiveTableName(options.getString(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), ""), dbName, studyId);
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
        return buildTableName(namespace, dbName, ARCHIVE_SUFIX + studyId);
    }

    public static int getStudyIdFromArchiveTable(String archiveTable) {
        int idx = archiveTable.lastIndexOf(ARCHIVE_SUFIX.charAt(ARCHIVE_SUFIX.length() - 1));
        return Integer.valueOf(archiveTable.substring(idx + 1));
    }

    public static String getSampleIndexTableName(String namespace, String dbName, int studyId) {
        return buildTableName(namespace, dbName, SAMPLE_SUFIX + studyId);
    }

    public static String getVariantTableName(String dbName, ObjectMap options) {
        return getVariantTableName(options.getString(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), ""), dbName);
    }

    public static String getVariantTableName(String dbName, Configuration conf) {
        return getVariantTableName(conf.get(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), ""), dbName);
    }

    public static String getVariantTableName(String namespace, String dbName) {
        return buildTableName(namespace, dbName, VARIANTS_SUFIX);
    }

    public static String getAnnotationIndexTableName(String namespace, String dbName) {
        return buildTableName(namespace, dbName, ANNOTATION_SUFIX);
    }

    public static String getPendingAnnotationTableName(String namespace, String dbName) {
        return buildTableName(namespace, dbName, PENDING_ANNOTATION_SUFIX);
    }

    public static String getPendingSecondaryIndexTableName(String namespace, String dbName) {
        return buildTableName(namespace, dbName, PENDING_SECONDARY_INDEX_SUFIX);
    }

    public static String getMetaTableName(String namespace, String dbName) {
        return buildTableName(namespace, dbName, META_SUFIX);
    }

    protected static String buildTableName(String namespace, String dbName, String tableName) {
        StringBuilder sb = new StringBuilder();

        if (StringUtils.isNotEmpty(namespace)) {
            if (dbName.contains(":")) {
                if (!dbName.startsWith(namespace + ":")) {
                    throw new IllegalArgumentException("Wrong namespace : '" + dbName + "'."
                            + " Namespace mismatches with the read from configuration:" + namespace);
                } else {
                    dbName = dbName.substring(dbName.indexOf(':') + 1); // Remove '<namespace>:'
                }
            }
            sb.append(namespace).append(':');
        }
        if (StringUtils.isNotEmpty(dbName)) {
            if (dbName.endsWith("_") && tableName.startsWith("_")) {
                sb.append(dbName, 0, dbName.length() - 1);
            } else {
                sb.append(dbName);
            }
            if (!dbName.endsWith("_") && !tableName.startsWith("_")) {
                sb.append("_");
            }
        }
        sb.append(tableName);

        String fullyQualified = sb.toString();
        TableName.isLegalFullyQualifiedTableName(fullyQualified.getBytes());
        return fullyQualified;
    }

}
