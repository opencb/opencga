package org.opencb.opencga.storage.hadoop.variant.converters;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.*;

public class VariantRow {

    private final Result result;
    private final ResultSet resultSet;
    private Variant variant;
    private VariantAnnotation variantAnnotation;

    public VariantRow(Result result) {
        this.result = Objects.requireNonNull(result);
        this.resultSet = null;
    }

    public VariantRow(ResultSet resultSet) {
        this.resultSet = Objects.requireNonNull(resultSet);
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

    public VariantRowWalker walker() {
        return new VariantRowWalker();
    }

    private void walkVariant(VariantRowWalker walker) {
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
                        walker.file(new BytesFileColumn(bytes, extractStudyId(columnName), extractFileId(columnName)));
                    } else if (columnName.endsWith(SAMPLE_DATA_SUFIX)) {
                        walker.sample(new BytesSampleColumn(bytes, extractStudyId(columnName), extractSampleId(columnName)));
                    } else if (columnName.endsWith(STUDY_SUFIX)) {
                        walker.study(extractStudyId(columnName));
                    } else if (columnName.endsWith(COHORT_STATS_PROTOBUF_SUFFIX)) {
                        walker.stats(new BytesStatsColumn(bytes, extractStudyId(columnName), extractCohortStatsId(columnName)));
                    } else if (columnName.endsWith(VARIANT_SCORE_SUFIX)) {
                        walker.score(new BytesVariantScoreColumn(bytes, extractStudyId(columnName), extractScoreId(columnName)));
                    } else if (columnName.endsWith(FILL_MISSING_SUFIX)) {
                        int studyId = Integer.valueOf(columnName.split("_")[1]);
                        walker.fillMissing(studyId, resultSet.getInt(i));
                    }
                }
            } catch (SQLException e) {
                throw VariantQueryException.internalException(e);
            }
        } else {
            for (Cell cell : result.rawCells()) {
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                if (columnName.endsWith(FILE_SUFIX)) {
                    walker.file(new BytesFileColumn(cell, extractStudyId(columnName), extractFileId(columnName)));
                } else if (columnName.endsWith(SAMPLE_DATA_SUFIX)) {
                    walker.sample(new BytesSampleColumn(cell, extractStudyId(columnName), extractSampleId(columnName)));
                } else if (columnName.endsWith(STUDY_SUFIX)) {
                    walker.study(extractStudyId(columnName));
                } else if (columnName.endsWith(COHORT_STATS_PROTOBUF_SUFFIX)) {
                        walker.stats(new BytesStatsColumn(cell, extractStudyId(columnName), extractCohortStatsId(columnName)));
                } else if (columnName.endsWith(VARIANT_SCORE_SUFIX)) {
                        walker.score(new BytesVariantScoreColumn(cell, extractStudyId(columnName), extractScoreId(columnName)));
                } else if (columnName.endsWith(FILL_MISSING_SUFIX)) {
                    int studyId = Integer.valueOf(columnName.split("_")[1]);
                    walker.fillMissing(studyId,
                            ((Integer) PInteger.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
                }
            }
        }
    }

    public class VariantRowWalker {

        private IntConsumer studyConsumer = r -> { };
        private Consumer<FileColumn> fileConsumer = r -> { };
        private Consumer<SampleColumn> sampleConsumer = r -> { };
        private Consumer<StatsColumn> statsConsumer = r -> { };
        private Consumer<VariantScoreColumn> variantScoreConsumer = r -> { };
        private BiConsumer<Integer, Integer> fillMissingConsumer = (k, v) -> { };
        private Variant variant;

        private void setVariant(Variant variant) {
            this.variant = variant;
        }

        public Variant getVariant() {
            return variant;
        }

        protected void study(int studyId) {
            studyConsumer.accept(studyId);
        }

        protected void file(FileColumn fileColumn) {
            fileConsumer.accept(fileColumn);
        }

        protected void sample(SampleColumn sampleColumn) {
            sampleConsumer.accept(sampleColumn);
        }

        protected void stats(StatsColumn statsColumn) {
            statsConsumer.accept(statsColumn);
        }

        protected void score(VariantScoreColumn scoreColumn) {
            variantScoreConsumer.accept(scoreColumn);
        }

        protected void fillMissing(int studyId, int fillMissing) {
            fillMissingConsumer.accept(studyId, fillMissing);
        }

        public Variant walk() {
            walkVariant(this);
            return getVariant();
        }

        public VariantRowWalker onStudy(IntConsumer consumer) {
            studyConsumer = consumer;
            return this;
        }

        public VariantRowWalker onFile(Consumer<FileColumn> consumer) {
            fileConsumer = consumer;
            return this;
        }

        public VariantRowWalker onSample(Consumer<SampleColumn> consumer) {
            sampleConsumer = consumer;
            return this;
        }

        public VariantRowWalker onCohortStats(Consumer<StatsColumn> consumer) {
            statsConsumer = consumer;
            return this;
        }

        public VariantRowWalker onVariantScore(Consumer<VariantScoreColumn> consumer) {
            variantScoreConsumer = consumer;
            return this;
        }

        public VariantRowWalker onFillMissing(BiConsumer<Integer, Integer> consumer) {
            fillMissingConsumer = consumer;
            return this;
        }
    }

    public interface FileColumn {
        int getStudyId();

        int getFileId();

        PhoenixArray raw();

        String getCall();

        List<AlternateCoordinate> getSecondaryAlternates();

        VariantOverlappingStatus getOverlappingStatus();

        Double getQual();

        String getQualString();

        String getFilter();

        String getString(int idx);
    }

    public interface SampleColumn {
        int getStudyId();

        int getSampleId();

        List<String> getSampleData();

        List<String> getMutableSampleData();

        default String getGT() {
            return getSampleData(0);
        }

        String getSampleData(int idx);

    }

    public interface StatsColumn {
        int getStudyId();

        int getCohortId();

        VariantProto.VariantStats toProto();
    }

    public interface VariantScoreColumn {
        int getStudyId();

        int getScoreId();

        float getScore();

        float getPValue();
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
            return get(arrayIndex, PVarchar.INSTANCE);
        }

        public float getFloat(int arrayIndex) {
            return get(arrayIndex, PFloat.INSTANCE);
        }

        private <T> T get(int arrayIndex, PDataType<T> pDataType) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable(
                    valueArray,
                    valueOffset,
                    valueLength);
            PhoenixHelper.positionAtArrayElement(ptr, arrayIndex, pDataType, null);
            return (T) pDataType.toObject(ptr);
        }
    }

    private static class BytesSampleColumn extends BytesColumn implements SampleColumn {
        private final int studyId;
        private final int sampleId;

//        public static SampleColumn getOrNull(Cell cell) {
//            Integer sampleId = VariantPhoenixHelper.extractSampleId(
//                    cell.getQualifierArray(),
//                    cell.getQualifierOffset(),
//                    cell.getQualifierLength());
//            if (sampleId == null) {
//                return null;
//            } else {
//                return new BytesSampleColumn(cell, sampleId);
//            }
//        }

        BytesSampleColumn(Cell cell, int studyId, int sampleId) {
            super(cell);
            this.studyId = studyId;
            this.sampleId = sampleId;
        }

        BytesSampleColumn(byte[] value, int studyId, int sampleId) {
            super(value);
            this.studyId = studyId;
            this.sampleId = sampleId;
        }

        @Override
        public int getStudyId() {
            return studyId;
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
        public List<String> getMutableSampleData() {
            PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(
                    valueArray, valueOffset, valueLength);
            return AbstractPhoenixConverter.toModifiableList(array);
//            return AbstractPhoenixConverter.toList(array);
        }

        @Override
        public String getSampleData(int idx) {
            return super.getString(idx);
        }
    }

    private static class BytesFileColumn extends BytesColumn implements FileColumn {
        private final int studyId;
        private final int fileId;

//        public static FileColumn getOrNull(Cell cell) {
//            Integer fileId = VariantPhoenixHelper.extractFileId(
//                    cell.getQualifierArray(),
//                    cell.getQualifierOffset(),
//                    cell.getQualifierLength());
//            if (fileId == null) {
//                return null;
//            } else {
//                return new BytesFileColumn(cell, fileId);
//            }
//        }

        BytesFileColumn(Cell cell, int studyId, int fileId) {
            super(cell);
            this.studyId = studyId;
            this.fileId = fileId;
        }

        BytesFileColumn(byte[] value, int studyId, int fileId) {
            super(value);
            this.studyId = studyId;
            this.fileId = fileId;
        }

        @Override
        public int getStudyId() {
            return studyId;
        }

        @Override
        public int getFileId() {
            return fileId;
        }

        @Override
        public PhoenixArray raw() {
            return (PhoenixArray) PVarcharArray.INSTANCE.toObject(
                    valueArray, valueOffset, valueLength);
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
            String qualStr = getQualString();
            if (StringUtils.isNotEmpty(qualStr) && !(".").equals(qualStr)) {
                return Double.valueOf(qualStr);
            } else {
                return null;
            }
        }

        @Override
        public String getQualString() {
            return getString(HBaseToStudyEntryConverter.FILE_QUAL_IDX);
        }

        @Override
        public String getFilter() {
            return getString(HBaseToStudyEntryConverter.FILE_FILTER_IDX);
        }
    }

    public static class BytesStatsColumn extends BytesColumn implements StatsColumn {
        private final int studyId;
        private final int cohortId;

        public BytesStatsColumn(Cell cell, int studyId, int cohortId) {
            super(cell);
            this.studyId = studyId;
            this.cohortId = cohortId;
        }

        public BytesStatsColumn(byte[] value, int studyId, int cohortId) {
            super(value);
            this.studyId = studyId;
            this.cohortId = cohortId;
        }

        @Override
        public int getStudyId() {
            return studyId;
        }

        @Override
        public int getCohortId() {
            return cohortId;
        }

        @Override
        public VariantProto.VariantStats toProto() {
            try {
                return VariantProto.VariantStats.parseFrom(ByteBuffer.wrap(valueArray, valueOffset, valueLength));
            } catch (InvalidProtocolBufferException e) {
                throw VariantQueryException.internalException(e);
            }
        }
    }

    public static class BytesVariantScoreColumn extends BytesColumn implements VariantScoreColumn {
        private final int studyId;
        private final int scoreId;

        public BytesVariantScoreColumn(Cell cell, int studyId, int scoreId) {
            super(cell);
            this.studyId = studyId;
            this.scoreId = scoreId;
        }

        public BytesVariantScoreColumn(byte[] value, int studyId, int scoreId) {
            super(value);
            this.studyId = studyId;
            this.scoreId = scoreId;
        }

        @Override
        public int getStudyId() {
            return studyId;
        }

        @Override
        public int getScoreId() {
            return scoreId;
        }

        @Override
        public float getScore() {
            return getFloat(0);
        }

        @Override
        public float getPValue() {
            return getFloat(1);
        }
    }

}
