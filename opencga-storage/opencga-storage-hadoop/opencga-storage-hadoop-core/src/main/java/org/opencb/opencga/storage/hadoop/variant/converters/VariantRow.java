package org.opencb.opencga.storage.hadoop.variant.converters;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.*;

public class VariantRow {

    private final Result result;
    private final ResultSet resultSet;
    private Variant variant;
    private VariantAnnotation variantAnnotation;

    public VariantRow(Result result) {
        this.result = result;
        this.resultSet = null;
    }

    public VariantRow(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.result = null;
    }

    public Variant getVariant() {
        if (variant == null) {
            if (result != null) {
                variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());
            } else {
                variant = VariantPhoenixKeyFactory.extractVariantFromResultSet(resultSet);
            }
        }
        return variant;
    }

    public VariantAnnotation getVariantAnnotation() {
        if (variantAnnotation == null) {
            HBaseToVariantAnnotationConverter c = new HBaseToVariantAnnotationConverter(GenomeHelper.DEFAULT_COLUMN_FAMILY_BYTES, -1);
            if (result != null) {
                variantAnnotation = c.convert(result);
            } else {
                variantAnnotation = c.convert(resultSet);
            }
        }
        return variantAnnotation;
    }

    public void forEachSample(Consumer<SampleColumn> consumer) {
        walker().onSample(consumer).walk();
    }

    public ResultWalker walker() {
        return new ResultWalker();
    }

    private void walkVariant(ResultWalker walker) {
        walker.setVariant(getVariant());
        if (resultSet != null) {
            try {
                ResultSetMetaData metaData = resultSet.getMetaData();

                int columnCount = metaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    byte[] bytes = resultSet.getBytes(i);
                    if (bytes == null) {
                        continue;
                    }
                    if (columnName.endsWith(FILE_SUFIX)) {
                        walker.file(new BytesFileColumn(bytes, extractFileId(columnName, true)));
                    } else if (columnName.endsWith(SAMPLE_DATA_SUFIX)) {
                        walker.sample(new BytesSampleColumn(bytes, extractSampleId(columnName, true)));
                    } else if (columnName.endsWith(STUDY_SUFIX)) {
                        walker.study(extractStudyId(columnName, true));
//                    } else if (columnName.endsWith(COHORT_STATS_PROTOBUF_SUFFIX)) {
                    } else if (columnName.endsWith(FILL_MISSING_SUFIX)) {
                        walker.fillMissing(resultSet.getInt(i));
                    }
                }
            } catch (SQLException e) {
                throw VariantQueryException.internalException(e);
            }
        } else {
            for (Cell cell : result.rawCells()) {
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                if (columnName.endsWith(FILE_SUFIX)) {
                    walker.file(new BytesFileColumn(cell, extractFileId(columnName, true)));
                } else if (columnName.endsWith(SAMPLE_DATA_SUFIX)) {
                    walker.sample(new BytesSampleColumn(cell, extractSampleId(columnName, true)));
                } else if (columnName.endsWith(STUDY_SUFIX)) {
                    walker.study(extractStudyId(columnName, true));
//                } else if (columnName.endsWith(COHORT_STATS_PROTOBUF_SUFFIX)) {
                } else if (columnName.endsWith(FILL_MISSING_SUFIX)) {
                    walker.fillMissing(Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
        }
    }

    public class ResultWalker {

        private Consumer<FileColumn> fileConsumer = r -> { };
        private Consumer<SampleColumn> sampleConsumer = r -> { };
        private Consumer<Integer> fillMissingConsumer = r -> { };

        private Variant variant;

        private void setVariant(Variant variant) {
            this.variant = variant;
        }

        public Variant getVariant() {
            return variant;
        }

        protected void study(int studyId) {
        }

        protected void file(FileColumn fileColumn) {
            fileConsumer.accept(fileColumn);
        }

        protected void sample(SampleColumn sampleColumn) {
            sampleConsumer.accept(sampleColumn);
        }

        protected void fillMissing(int fillMissing) {
            fillMissingConsumer.accept(fillMissing);
        }

        public void walk() {
            walkVariant(this);
        }

        public ResultWalker onFile(Consumer<FileColumn> consumer) {
            fileConsumer = consumer;
            return this;
        }

        public ResultWalker onSample(Consumer<SampleColumn> consumer) {
            sampleConsumer = consumer;
            return this;
        }

        public ResultWalker onFillMissing(Consumer<Integer> consumer) {
            fillMissingConsumer = consumer;
            return this;
        }
    }

    public interface FileColumn {
        int getFileId();

        String getCall();

        List<AlternateCoordinate> getSecondaryAlternates();

        VariantOverlappingStatus getOverlappingStatus();

        Double getQual();

        String getFilter();

        String getString(int idx);
    }

    public interface SampleColumn {
        int getSampleId();

        List<String> getSampleData();

        default String getGT() {
            return getSampleData(0);
        }

        String getSampleData(int idx);

    }

    public interface StatsColumn {
        int getStudyId();

        int getCohortId();
    }

    private static class BytesColumn {
        protected final byte[] valueArray;
        protected final int valueOffset;
        protected final int valueLength;

        BytesColumn(Cell cell) {
            valueArray = cell.getValueArray();
            valueOffset = cell.getValueOffset();
            valueLength = cell.getValueLength();
        }

        BytesColumn(byte[] value) {
            valueArray = value;
            valueOffset = 0;
            valueLength = value.length;
        }

        public String getString(int arrayIndex) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable(
                    valueArray,
                    valueOffset,
                    valueLength);
            PhoenixHelper.positionAtArrayElement(ptr, arrayIndex, PVarchar.INSTANCE, null);
            return Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());
        }
    }

    private static class BytesSampleColumn extends BytesColumn implements SampleColumn {
        private final int sampleId;

        public static SampleColumn getOrNull(Cell cell) {
            Integer sampleId = VariantPhoenixHelper.extractSampleId(
                    cell.getQualifierArray(),
                    cell.getQualifierOffset(),
                    cell.getQualifierLength());
            if (sampleId == null) {
                return null;
            } else {
                return new BytesSampleColumn(cell, sampleId);
            }
        }

        BytesSampleColumn(Cell cell, int sampleId) {
            super(cell);
            this.sampleId = sampleId;
        }

        BytesSampleColumn(byte[] value, int sampleId) {
            super(value);
            this.sampleId = sampleId;
        }

        @Override
        public int getSampleId() {
            return sampleId;
        }

        @Override
        public List<String> getSampleData() {
            PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(
                    valueArray, valueOffset, valueLength);
//            return AbstractPhoenixConverter.toModifiableList(array);
            return AbstractPhoenixConverter.toList(array);
        }

        @Override
        public String getSampleData(int idx) {
            return super.getString(idx);
        }
    }

    private static class BytesFileColumn extends BytesColumn implements FileColumn {
        private final int fileId;

        public static FileColumn getOrNull(Cell cell) {
            Integer fileId = VariantPhoenixHelper.extractFileId(
                    cell.getQualifierArray(),
                    cell.getQualifierOffset(),
                    cell.getQualifierLength());
            if (fileId == null) {
                return null;
            } else {
                return new BytesFileColumn(cell, fileId);
            }
        }

        BytesFileColumn(Cell cell, int fileId) {
            super(cell);
            this.fileId = fileId;
        }

        BytesFileColumn(byte[] value, int fileId) {
            super(value);
            this.fileId = fileId;
        }

        @Override
        public int getFileId() {
            return fileId;
        }

        @Override
        public String getCall() {
            return getString(HBaseToStudyEntryConverter.FILE_CALL_IDX);
        }

        @Override
        public List<AlternateCoordinate> getSecondaryAlternates() {
            String secAlt = getString(HBaseToStudyEntryConverter.FILE_SEC_ALTS_IDX);
            if (StringUtils.isNotEmpty(secAlt)) {
                return HBaseToStudyEntryConverter.getAlternateCoordinates(secAlt);
            }
            return null;
        }

        @Override
        public VariantOverlappingStatus getOverlappingStatus() {
            return VariantOverlappingStatus.valueFromShortString(getString(HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX));
        }

        @Override
        public Double getQual() {
            String qualStr = getString(HBaseToStudyEntryConverter.FILE_QUAL_IDX);
            if (StringUtils.isNotEmpty(qualStr) && !(".").equals(qualStr)) {
                return Double.valueOf(qualStr);
            } else {
                return null;
            }
        }

        @Override
        public String getFilter() {
            return getString(HBaseToStudyEntryConverter.FILE_FILTER_IDX);
        }
    }

}
