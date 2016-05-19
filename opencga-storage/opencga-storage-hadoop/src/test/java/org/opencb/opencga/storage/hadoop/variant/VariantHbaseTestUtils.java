/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

import java.net.URI;
import java.util.Map;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantHbaseTestUtils {

    public static VariantHadoopDBAdaptor printVariantsFromArchiveTable(VariantHadoopDBAdaptor dbAdaptor,
            StudyConfiguration studyConfiguration) throws Exception {
        GenomeHelper helper = dbAdaptor.getGenomeHelper();
        helper.getHBaseManager().act(HadoopVariantStorageManager.getTableName(studyConfiguration.getStudyId()), table -> {
            for (Result result : table.getScanner(helper.getColumnFamily())) {
                try {
                    byte[] value = result.getValue(helper.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
                    if (value != null) {
                        System.out.println(VariantTableStudyRowsProto.parseFrom(value));
                    }
                } catch (Exception e) {
                    System.out.println("e.getMessage() = " + e.getMessage());
                }
            }
            return 0;
        });
        return dbAdaptor;
    }

    public static void removeFile(HadoopVariantStorageManager variantStorageManager, String dbName, URI outputUri, int fileId,
            StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        ObjectMap params = new ObjectMap(VariantStorageManager.Options.STUDY_CONFIGURATION.key(), studyConfiguration)
                .append(VariantStorageManager.Options.STUDY_ID.key(), studyConfiguration.getStudyId())
                .append(VariantStorageManager.Options.DB_NAME.key(), dbName).append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantAnnotationManager.SPECIES, "hsapiens").append(VariantAnnotationManager.ASSEMBLY, "GRc37")
                .append(HadoopVariantStorageManager.HADOOP_DELETE_FILE, true);
        if (otherParams != null) {
            params.putAll(otherParams);
        }

        variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions()
                .putAll(params);
        variantStorageManager.dropFile(studyConfiguration.getStudyName(), fileId);
//        return variantStorageManager.readVariantSource(etlResult.getTransformResult(), new ObjectMap());
    }

    public static VariantSource loadFile(HadoopVariantStorageManager variantStorageManager, String dbName, URI outputUri,
                                         String resourceName, int fileId, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams, boolean doTransform, boolean loadArchive, boolean loadVariant) throws Exception {
        URI fileInputUri = VariantStorageManagerTestUtils.getResourceUri(resourceName);

        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "proto")
                .append(VariantStorageManager.Options.STUDY_CONFIGURATION.key(), studyConfiguration)
                .append(VariantStorageManager.Options.STUDY_ID.key(), studyConfiguration.getStudyId())
                .append(VariantStorageManager.Options.STUDY_NAME.key(), studyConfiguration.getStudyName())
                .append(VariantStorageManager.Options.DB_NAME.key(), dbName).append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantAnnotationManager.SPECIES, "hsapiens").append(VariantAnnotationManager.ASSEMBLY, "GRc37")
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_DIRECT, true)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_ARCHIVE, loadArchive)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, loadVariant);

        if (otherParams != null) {
            params.putAll(otherParams);
        }
        if (fileId > 0) {
            params.append(VariantStorageManager.Options.FILE_ID.key(), fileId);
        }
        StorageETLResult etlResult = VariantStorageManagerTestUtils.runETL(variantStorageManager, fileInputUri, outputUri, params, doTransform, doTransform, true);
        StudyConfiguration updatedStudyConfiguration = variantStorageManager.getDBAdaptor().getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        studyConfiguration.setIndexedFiles(updatedStudyConfiguration.getIndexedFiles());
        studyConfiguration.setAttributes(updatedStudyConfiguration.getAttributes());

        return variantStorageManager.readVariantSource(doTransform ? etlResult.getTransformResult() : etlResult.getInput());
    }

    public static VariantSource loadFile(HadoopVariantStorageManager variantStorageManager, String dbName, URI outputUri,
            String resourceName, int fileId, StudyConfiguration studyConfiguration) throws Exception {
        return loadFile(variantStorageManager, dbName, outputUri, resourceName, fileId, studyConfiguration, null, true, true, true);
    }

    public static VariantSource loadFile(HadoopVariantStorageManager variantStorageManager, String dbName, URI outputUri,
            String resourceName, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(variantStorageManager, dbName, outputUri, resourceName, -1, studyConfiguration, otherParams, true, true, true);
    }
}
