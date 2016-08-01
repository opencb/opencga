/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getTmpRootDir;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManagerTestUtils.configuration;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantHbaseTestUtils {

    public static VariantHadoopDBAdaptor printVariantsFromArchiveTable(VariantHadoopDBAdaptor dbAdaptor,
            StudyConfiguration studyConfiguration) throws Exception {
        GenomeHelper helper = dbAdaptor.getGenomeHelper();
        helper.getHBaseManager().act(HadoopVariantStorageManager.getArchiveTableName(studyConfiguration.getStudyId(), dbAdaptor.getConfiguration()), table -> {
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

    public static void printVariantsFromVariantsTable(VariantHadoopDBAdaptor dbAdaptor) throws IOException {
        String tableName = HadoopVariantStorageManager.getVariantTableName(VariantStorageManagerTestUtils.DB_NAME, dbAdaptor.getConfiguration());
        System.out.println("Query from HBase : " + tableName);
        HBaseManager hm = new HBaseManager(configuration.get());
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        Path outputFile = getTmpRootDir().resolve("variant_table_hbase_" + TimeUtils.getTimeMillis() + ".txt");
        System.out.println("Variant table file = " + outputFile);
        PrintStream os = new PrintStream(new FileOutputStream(outputFile.toFile()));
        int numVariants = hm.act(tableName, table -> {
            int num = 0;
            ResultScanner resultScanner = table.getScanner(genomeHelper.getColumnFamily());
            for (Result result : resultScanner) {
                if (Bytes.toString(result.getRow()).startsWith(genomeHelper.getMetaRowKeyString())) {
                    continue;
                }
                Variant variant = genomeHelper.extractVariantFromVariantRowKey(result.getRow());
                os.println("Variant = " + variant);
                for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(genomeHelper.getColumnFamily()).entrySet()) {
                    String key = Bytes.toString(entry.getKey());
                    VariantPhoenixHelper.Column column = VariantPhoenixHelper.VariantColumn.getColumn(key);
                    if (column != null) {
                        os.println("\t" + key + " = " + length(entry.getValue()) + ", "
                                + column.getPDataType().toObject(entry.getValue()));
                    } else if (key.endsWith(VariantPhoenixHelper.STATS_PROTOBUF_SUFIX)
                            || key.endsWith("_" + VariantTableStudyRow.FILTER_OTHER)
                            || key.endsWith("_" + VariantTableStudyRow.COMPLEX)) {
                        os.println("\t" + key + " = " + length(entry.getValue()) + ", " + Arrays.toString(entry.getValue()));
                    } else if (key.startsWith(VariantPhoenixHelper.POPULATION_FREQUENCY_PREFIX)) {
                        os.println("\t" + key + " = " + length(entry.getValue()) + ", " + PFloatArray.INSTANCE.toObject(entry.getValue()));
                    } else if (key.endsWith("_" + VariantTableStudyRow.HET_REF)
                            || key.endsWith("_" + VariantTableStudyRow.HOM_VAR)
                            || key.endsWith("_" + VariantTableStudyRow.NOCALL)
                            || key.endsWith("_" + VariantTableStudyRow.OTHER)) {
                        os.println("\t" + key + " = " +  PUnsignedIntArray.INSTANCE.toObject(entry.getValue()));
                    } else if (key.endsWith("_" + VariantTableStudyRow.HOM_REF)
                            || key.endsWith("_" + VariantTableStudyRow.CALL_CNT)
                            || key.endsWith("_" + VariantTableStudyRow.PASS_CNT)) {
                        os.println("\t" + key + " = " + PUnsignedInt.INSTANCE.toObject(entry.getValue()));
                    } else if (key.endsWith(VariantPhoenixHelper.MAF_SUFIX)
                            || key.endsWith(VariantPhoenixHelper.MGF_SUFIX)) {
                        os.println("\t" + key + " = " + PFloat.INSTANCE.toObject(entry.getValue()));
                    } else if (entry.getValue().length == 4) {
                        Object o = null;
                        try {
                            o = PUnsignedInt.INSTANCE.toObject(entry.getValue());
                        } catch (IllegalDataException ignore) {}
                        os.println("\t" + key + " = "
                                + PInteger.INSTANCE.toObject(entry.getValue()) + " , "
                                + o + " , "
                                + PFloat.INSTANCE.toObject(entry.getValue()) + " , ");
                    } else {
                        os.println("\t" + key + " ~ " + length(entry.getValue()) + ", "
                                + Bytes.toString(entry.getValue()));
                    }

                }
                os.println("--------------------");
                if (!variant.getChromosome().equals(genomeHelper.getMetaRowKeyString())) {
                    num++;
                }
            }
            os.close();
            resultScanner.close();
            return num;
        });
    }

    private static String length(byte[] array) {
        return "(" + array.length + " B)";
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
        studyConfiguration.copy(variantStorageManager.getDBAdaptor().getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first());
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
        studyConfiguration.copy(updatedStudyConfiguration);

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
