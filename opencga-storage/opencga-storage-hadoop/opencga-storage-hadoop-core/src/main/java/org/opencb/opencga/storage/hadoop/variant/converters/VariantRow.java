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
import org.opencb.biodata.models.variant.avro.OriginalCall;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.*;

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

    public static VariantRowWalkerBuilder walker(Result result) {
        return new VariantRow(result).walker();
    }

    public static VariantRowWalkerBuilder walker(ResultSet resultSet) {
        return new VariantRow(resultSet).walker();
    }

    public Variant getVariant() {
        if (variant == null) {
            if (result != null) {
                byte[] row = result.getRow();
                Objects.requireNonNull(row, "Empty result. Missing variant rowkey.");
                variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(row);
            } else {
                variant = VariantPhoenixKeyFactory.extractVariantFromResultSet(resultSet);
            }
        }
        return variant;
    }

    public VariantAnnotation getVariantAnnotation(HBaseToVariantAnnotationConverter converter) {
        if (variantAnnotation == null) {
            if (result != null) {
                variantAnnotation = converter.convert(result);
            } else {
                variantAnnotation = converter.convert(resultSet);
            }
        }
        return variantAnnotation;
    }

    public void forEachSample(Consumer<SampleColumn> consumer) {
        walker().onSample(consumer).walk();
    }

    public void forEachFile(Consumer<FileColumn> consumer) {
        walker().onFile(consumer).walk();
    }

    public Set<Integer> getStudies() {
        Set<Integer> studies = new HashSet<>();
        walker().onStudy(studies::add).walk();
        return studies;
    }

    public VariantRowWalkerBuilder walker() {
        return new VariantRowWalkerBuilder();
    }

    public void walk(VariantRowWalker walker) {
        walk(walker, true, true, true, true, true);
    }

    protected void walk(VariantRowWalker walker, boolean file, boolean sample, boolean cohort, boolean score, boolean annotation) {
        walker.variant(getVariant());
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
                    if (file && columnName.endsWith(FILE_SUFIX)) {
                        walker.file(new BytesFileColumn(bytes, extractStudyId(columnName), extractFileId(columnName)));
                    } else if (sample && columnName.endsWith(SAMPLE_DATA_SUFIX)) {
                        walker.sample(new BytesSampleColumn(bytes, extractStudyId(columnName), extractSampleId(columnName),
                                extractFileIdFromSampleColumn(columnName, false)));
                    } else if (columnName.endsWith(STUDY_SUFIX)) {
                        walker.study(extractStudyId(columnName));
                    } else if (cohort && columnName.endsWith(COHORT_STATS_PROTOBUF_SUFFIX)) {
                        walker.stats(new BytesStatsColumn(bytes, extractStudyId(columnName), extractCohortStatsId(columnName)));
                    } else if (score && columnName.endsWith(VARIANT_SCORE_SUFIX)) {
                        walker.score(new BytesVariantScoreColumn(bytes, extractStudyId(columnName), extractScoreId(columnName)));
                    } else if (columnName.endsWith(FILL_MISSING_SUFIX)) {
                        int studyId = Integer.parseInt(columnName.split("_")[1]);
                        walker.fillMissing(studyId, resultSet.getInt(i));
                    } else if (annotation && columnName.equals(VariantColumn.FULL_ANNOTATION.column())) {
                        walker.variantAnnotation(new BytesVariantAnnotationColumn(bytes));
                    }
                }
            } catch (SQLException e) {
                throw VariantQueryException.internalException(e);
            }
        } else {
            for (Cell cell : result.rawCells()) {
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                if (file && columnName.endsWith(FILE_SUFIX)) {
                    walker.file(new BytesFileColumn(cell, extractStudyId(columnName), extractFileId(columnName)));
                } else if (sample && columnName.endsWith(SAMPLE_DATA_SUFIX)) {
                    walker.sample(new BytesSampleColumn(cell, extractStudyId(columnName), extractSampleId(columnName),
                            extractFileIdFromSampleColumn(columnName, false)));
                } else if (columnName.endsWith(STUDY_SUFIX)) {
                    walker.study(extractStudyId(columnName));
                } else if (cohort && columnName.endsWith(COHORT_STATS_PROTOBUF_SUFFIX)) {
                        walker.stats(new BytesStatsColumn(cell, extractStudyId(columnName), extractCohortStatsId(columnName)));
                } else if (score && columnName.endsWith(VARIANT_SCORE_SUFIX)) {
                        walker.score(new BytesVariantScoreColumn(cell, extractStudyId(columnName), extractScoreId(columnName)));
                } else if (columnName.endsWith(FILL_MISSING_SUFIX)) {
                    int studyId = Integer.parseInt(columnName.split("_")[1]);
                    walker.fillMissing(studyId,
                            ((Integer) PInteger.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
                } else if (annotation && columnName.equals(VariantColumn.FULL_ANNOTATION.column())) {
                    walker.variantAnnotation(new BytesVariantAnnotationColumn(cell));
                } else if (columnName.equals(VariantColumn.TYPE.column())) {
                    walker.type(VariantType.valueOf(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
                }
            }
        }
    }

    public abstract static class VariantRowWalker {

        protected void variant(Variant variant) {
        }

        protected void type(VariantType type) {
        }

        protected void study(int studyId) {
        }

        protected void file(FileColumn fileColumn) {
        }

        protected void sample(SampleColumn sampleColumn) {
        }

        protected void stats(StatsColumn statsColumn) {
        }

        protected void score(VariantScoreColumn scoreColumn) {
        }

        protected void fillMissing(int studyId, int fillMissing) {
        }

        protected void variantAnnotation(VariantAnnotationColumn column) {
        }
    }

    public class VariantRowWalkerBuilder extends VariantRowWalker {

        private IntConsumer studyConsumer = r -> { };
        private Consumer<FileColumn> fileConsumer = r -> { };
        private boolean hasFileConsumer = false;
        private Consumer<SampleColumn> sampleConsumer = r -> { };
        private boolean hasSampleConsumer = false;
        private Consumer<StatsColumn> statsConsumer = r -> { };
        private boolean hasStatsConsumer = false;
        private Consumer<VariantScoreColumn> variantScoreConsumer = r -> { };
        private boolean hasVariantScoreConsumer = false;
        private BiConsumer<Integer, Integer> fillMissingConsumer = (k, v) -> { };
        private Consumer<VariantAnnotationColumn> variantAnnotationConsummer = (k) -> { };
        private boolean hasVariantAnnotationConsummer = false;
        private Variant variant;

        public Variant getVariant() {
            return variant;
        }

        @Override
        protected void variant(Variant variant) {
            this.variant = variant;
        }

        @Override
        protected void study(int studyId) {
            studyConsumer.accept(studyId);
        }

        @Override
        protected void file(FileColumn fileColumn) {
            fileConsumer.accept(fileColumn);
        }

        @Override
        protected void sample(SampleColumn sampleColumn) {
            sampleConsumer.accept(sampleColumn);
        }

        @Override
        protected void stats(StatsColumn statsColumn) {
            statsConsumer.accept(statsColumn);
        }

        @Override
        protected void score(VariantScoreColumn scoreColumn) {
            variantScoreConsumer.accept(scoreColumn);
        }

        @Override
        protected void fillMissing(int studyId, int fillMissing) {
            fillMissingConsumer.accept(studyId, fillMissing);
        }

        @Override
        protected void variantAnnotation(VariantAnnotationColumn column) {
            variantAnnotationConsummer.accept(column);
        }

        public Variant walk() {
            VariantRow.this.walk(this,
                    hasFileConsumer,
                    hasSampleConsumer,
                    hasStatsConsumer,
                    hasVariantScoreConsumer,
                    hasVariantAnnotationConsummer);
            return getVariant();
        }

        public VariantRowWalkerBuilder onStudy(IntConsumer consumer) {
            studyConsumer = consumer;
            return this;
        }

        public VariantRowWalkerBuilder onFile(Consumer<FileColumn> consumer) {
            fileConsumer = consumer;
            hasFileConsumer = true;
            return this;
        }

        public VariantRowWalkerBuilder onSample(Consumer<SampleColumn> consumer) {
            sampleConsumer = consumer;
            hasSampleConsumer = true;
            return this;
        }

        public VariantRowWalkerBuilder onCohortStats(Consumer<StatsColumn> consumer) {
            statsConsumer = consumer;
            hasStatsConsumer = true;
            return this;
        }

        public VariantRowWalkerBuilder onVariantScore(Consumer<VariantScoreColumn> consumer) {
            variantScoreConsumer = consumer;
            hasVariantScoreConsumer = true;
            return this;
        }

        public VariantRowWalkerBuilder onFillMissing(BiConsumer<Integer, Integer> consumer) {
            fillMissingConsumer = consumer;
            return this;
        }

        public VariantRowWalkerBuilder onVariantAnnotation(Consumer<VariantAnnotationColumn> consumer) {
            variantAnnotationConsummer = consumer;
            hasVariantAnnotationConsummer = true;
            return this;
        }
    }

    public interface FileColumn extends Column {
        int getStudyId();

        int getFileId();

        PhoenixArray raw();

        default String getCallString() {
            return getString(HBaseToStudyEntryConverter.FILE_CALL_IDX);
        }

        default OriginalCall getCall() {
            String callString = getCallString();
            if (callString == null) {
                return null;
            } else {
                int i = callString.lastIndexOf(':');
                return new OriginalCall(
                        callString.substring(0, i),
                        Integer.valueOf(callString.substring(i + 1)));
            }
        }

        default List<AlternateCoordinate> getSecondaryAlternates() {
            String secAlt = getString(HBaseToStudyEntryConverter.FILE_SEC_ALTS_IDX);
            if (StringUtils.isNotEmpty(secAlt)) {
                return HBaseToStudyEntryConverter.getAlternateCoordinates(secAlt);
            }
            return null;
        }

        default VariantOverlappingStatus getOverlappingStatus() {
            return VariantOverlappingStatus.valueFromShortString(getString(HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX));
        }

        default Double getQual() {
            String qualStr = getQualString();
            if (StringUtils.isNotEmpty(qualStr) && !(".").equals(qualStr)) {
                return Double.valueOf(qualStr);
            } else {
                return null;
            }
        }

        default String getQualString() {
            return getString(HBaseToStudyEntryConverter.FILE_QUAL_IDX);
        }

        default String getFilter() {
            return getString(HBaseToStudyEntryConverter.FILE_FILTER_IDX);
        }

        default String getFileData(int fileDataField) {
            return getString(fileDataField + HBaseToStudyEntryConverter.FILE_INFO_START_IDX);
        }

        default Float getFloatValue(int idx) {
            return toFloat(getString(idx));
        }

        String getString(int idx);
    }

    static Float toFloat(String value) {
        if (StringUtils.isNotEmpty(value) && !(".").equals(value)) {
            return Float.valueOf(value);
        } else {
            return null;
        }
    }

    public interface SampleColumn extends Column {
        int getStudyId();

        int getSampleId();

        Integer getFileId();

        List<String> getSampleData();

        List<String> getMutableSampleData();

        default String getGT() {
            return getSampleData(0);
        }

        String getSampleData(int idx);

        default Float getSampleDataFloat(int idx) {
            return toFloat(getSampleData(idx));
        }

    }

    public interface StatsColumn extends Column {
        int getStudyId();

        int getCohortId();

        VariantProto.VariantStats toProto();

        default VariantStats toJava() {
            return HBaseToVariantStatsConverter.convert(toProto());
        }
    }

    public interface VariantScoreColumn extends Column {
        int getStudyId();

        int getScoreId();

        float getScore();

        float getPValue();
    }

    public interface VariantAnnotationColumn extends Column {
        ImmutableBytesWritable toBytesWritable();
    }

    public interface Column {
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

        public ImmutableBytesWritable toBytesWritable() {
            return new ImmutableBytesWritable(valueArray, valueOffset, valueLength);
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
        private final Integer fileId;

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

        BytesSampleColumn(Cell cell, int studyId, int sampleId, Integer fileId) {
            super(cell);
            this.studyId = studyId;
            this.sampleId = sampleId;
            this.fileId = fileId;
        }

        BytesSampleColumn(byte[] value, int studyId, int sampleId, Integer fileId) {
            super(value);
            this.studyId = studyId;
            this.sampleId = sampleId;
            this.fileId = fileId;
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
        public Integer getFileId() {
            return fileId;
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

    }

    private static class BytesStatsColumn extends BytesColumn implements StatsColumn {
        private final int studyId;
        private final int cohortId;

        BytesStatsColumn(Cell cell, int studyId, int cohortId) {
            super(cell);
            this.studyId = studyId;
            this.cohortId = cohortId;
        }

        BytesStatsColumn(byte[] value, int studyId, int cohortId) {
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

    private static class BytesVariantScoreColumn extends BytesColumn implements VariantScoreColumn {
        private final int studyId;
        private final int scoreId;

        BytesVariantScoreColumn(Cell cell, int studyId, int scoreId) {
            super(cell);
            this.studyId = studyId;
            this.scoreId = scoreId;
        }

        BytesVariantScoreColumn(byte[] value, int studyId, int scoreId) {
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

    private static class BytesVariantAnnotationColumn extends BytesColumn implements VariantAnnotationColumn {

        BytesVariantAnnotationColumn(Cell cell) {
            super(cell);
        }

        BytesVariantAnnotationColumn(byte[] value) {
            super(value);
        }


    }

}
