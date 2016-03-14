/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.ETLResult;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

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
        if (fileId > 0) {
            params.append(VariantStorageManager.Options.FILE_ID.key(), fileId);
        }
        ETLResult etlResult = new ETLResult();

        variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions()
                .putAll(params);
        variantStorageManager.remove();
//        return variantStorageManager.readVariantSource(etlResult.transformResult, new ObjectMap());
    }

    private static VariantSource processParameters(HadoopVariantStorageManager variantStorageManager, URI outputUri, URI fileInputUri,
            int fileId, Map<? extends String, ?> otherParams, ObjectMap params) throws IOException, FileFormatException,
            StorageManagerException {
        if (otherParams != null) {
            params.putAll(otherParams);
        }
        if (fileId > 0) {
            params.append(VariantStorageManager.Options.FILE_ID.key(), fileId);
        }
        ETLResult etlResult = VariantStorageManagerTestUtils.runETL(variantStorageManager, fileInputUri, outputUri, params, params, params,
                params, params, params, params, true, true, true);

        return variantStorageManager.readVariantSource(etlResult.transformResult, new ObjectMap());
    }

    public static VariantSource loadFile(HadoopVariantStorageManager variantStorageManager, String dbName, URI outputUri,
            String resourceName, int fileId, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        URI fileInputUri = VariantStorageManagerTestUtils.getResourceUri(resourceName);
        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "avro")
                .append(VariantStorageManager.Options.STUDY_CONFIGURATION.key(), studyConfiguration)
                .append(VariantStorageManager.Options.STUDY_ID.key(), studyConfiguration.getStudyId())
                .append(VariantStorageManager.Options.DB_NAME.key(), dbName).append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantAnnotationManager.SPECIES, "hsapiens").append(VariantAnnotationManager.ASSEMBLY, "GRc37")
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_ARCHIVE, true)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, true);
        return processParameters(variantStorageManager, outputUri, fileInputUri, fileId, otherParams, params);
    }

    public static VariantSource loadFile(HadoopVariantStorageManager variantStorageManager, String dbName, URI outputUri,
            String resourceName, int fileId, StudyConfiguration studyConfiguration) throws Exception {
        return loadFile(variantStorageManager, dbName, outputUri, resourceName, fileId, studyConfiguration, null);
    }

    public static VariantSource loadFile(HadoopVariantStorageManager variantStorageManager, String dbName, URI outputUri,
            String resourceName, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(variantStorageManager, dbName, outputUri, resourceName, -1, studyConfiguration, otherParams);
    }
}
