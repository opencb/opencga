package org.opencb.opencga.storage.hadoop.variant.annotation.pending;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsTableBasedDescriptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.*;

public class AnnotationPendingVariantsDescriptor implements PendingVariantsTableBasedDescriptor {

    public static final byte[] FAMILY = GenomeHelper.COLUMN_FAMILY_BYTES;
    public static final byte[] COLUMN = Bytes.toBytes("v");
    public static final byte[] VALUE = new byte[0];
    private static final byte[] SO_BYTES = SO.bytes();
    private static final byte[] ANNOTATION_ID_BYTES = ANNOTATION_ID.bytes();

    private static Logger logger = LoggerFactory.getLogger(AnnotationPendingVariantsDescriptor.class);

    @Override
    public String name() {
        return "annotation";
    }

    public void checkValidPendingTableName(String tableName) {
        HBaseVariantTableNameGenerator.checkValidPendingAnnotationTableName(tableName);
    }

    public String getTableName(HBaseVariantTableNameGenerator generator) {
        return generator.getPendingAnnotationTableName();
    }

    public boolean createTableIfNeeded(String tableName, HBaseManager hBaseManager) throws IOException {
        return createTableIfNeeded(tableName, hBaseManager, Compression.getCompressionAlgorithmByName(
                hBaseManager.getConf().get(
                        HadoopVariantStorageOptions.PENDING_ANNOTATION_TABLE_COMPRESSION.key(),
                        HadoopVariantStorageOptions.PENDING_ANNOTATION_TABLE_COMPRESSION.defaultValue())));
    }

    public Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager) {
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, TYPE.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, ALLELES.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, SO.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, ANNOTATION_ID_BYTES);
        return scan;
    }


    public Function<Result, Mutation> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite) {
        // Resolve the project-wide annotationSetId once. A value of 0 means "no project metadata available
        // / first annotation never ran" — in that case fall back to the legacy "missing SO cell" check.
        ProjectMetadata projectMetadata = metadataManager.getProjectMetadata();
        final int projectAnnotationSetId;
        if (projectMetadata == null || projectMetadata.getAnnotation() == null
                || projectMetadata.getAnnotation().getCurrent() == null) {
            projectAnnotationSetId = 0;
        } else {
            projectAnnotationSetId = projectMetadata.getAnnotation().getCurrent().getId();
        }
        return value -> {
            byte[] alleles = null;
            if (overwrite || isPending(value, projectAnnotationSetId)) {
                for (Cell cell : value.rawCells()) {
                    if (cell.getValueLength() > 0) {
                        if (Bytes.equals(
                                cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                                ALLELES.bytes(), 0, ALLELES.bytes().length)) {
                            alleles = CellUtil.cloneValue(cell);
                        }
                    }
                }
                Put put = new Put(value.getRow());
                put.addColumn(FAMILY, COLUMN, VALUE);
                if (alleles != null) {
                    put.addColumn(FAMILY, ALLELES.bytes(), alleles);
                }
                return put;
            } else {
                return new Delete(value.getRow());
            }
        };
    }

    /**
     * A variant is pending annotation if any of these is true:
     * <ul>
     *   <li>It has no SO cell at all (never annotated).</li>
     *   <li>Its stored annotationSetId is older than the project's current annotationSetId
     *       (annotation has been overwritten with a different annotator since this row was written).</li>
     * </ul>
     * If the project's current annotationSetId can not be resolved ({@code projectAnnotationSetId == 0}),
     * only the legacy "missing SO" check is used — backcompat.
     */
    private boolean isPending(Result value, int projectAnnotationSetId) {
        boolean hasSo = false;
        Cell annotationIdCell = null;
        for (Cell cell : value.rawCells()) {
            if (cell.getValueLength() > 0) {
                if (Bytes.equals(
                        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        SO_BYTES, 0, SO_BYTES.length)) {
                    hasSo = true;
                } else if (Bytes.equals(
                        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        ANNOTATION_ID_BYTES, 0, ANNOTATION_ID_BYTES.length)) {
                    annotationIdCell = cell;
                }
            }
        }
        if (!hasSo) {
            // Never annotated.
            return true;
        }
        if (projectAnnotationSetId <= 1 || annotationIdCell == null) {
            // No drift possible (first annotation set, or row pre-dates ANNOTATION_ID stamping).
            return false;
        }
        byte[] annotationIdBytes = CellUtil.cloneValue(annotationIdCell);
        if (annotationIdBytes.length == 0) {
            return false;
        }
        Integer rowAnnotationSetId = (Integer) PInteger.INSTANCE.toObject(annotationIdBytes);
        return rowAnnotationSetId != null && rowAnnotationSetId < projectAnnotationSetId;
    }

}
