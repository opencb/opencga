package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitInputStream;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry.SampleIndexGtEntry;

import java.nio.ByteBuffer;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.*;

/**
 * Converts Results to SampleIndexEntry.
 * <p>
 * Created on 18/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToSampleIndexConverter implements Converter<Result, SampleIndexEntry> {

    private final SampleIndexVariantBiConverter converter;
    private final SampleIndexSchema schema;
    private final FileIndexSchema fileIndexSchema;
    private final FileDataSchema fileDataSchema;

    public HBaseToSampleIndexConverter(SampleIndexSchema schema) {
        this.schema = schema;
        converter = new SampleIndexVariantBiConverter(schema);
        fileIndexSchema = schema.getFileIndex();
        fileDataSchema = schema.getFileData();
    }

    @Override
    public SampleIndexEntry convert(Result result) {
        byte[] row = result.getRow();
        int sampleId = SampleIndexSchema.sampleIdFromRowKey(row);
        String chromosome = SampleIndexSchema.chromosomeFromRowKey(row);
        int batchStart = SampleIndexSchema.batchStartFromRowKey(row);

        SampleIndexEntry entry = new SampleIndexEntry(sampleId, chromosome, batchStart);

        for (Cell cell : result.rawCells()) {
            if (columnStartsWith(cell, META_PREFIX_BYTES)) {
                if (columnStartsWith(cell, GENOTYPE_COUNT_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, GENOTYPE_COUNT_PREFIX_BYTES))
                            .setCount(Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                } else if (columnStartsWith(cell, ANNOTATION_SUMMARY_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, ANNOTATION_SUMMARY_PREFIX_BYTES))
                            .setAnnotationIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, ANNOTATION_SUMMARY_COUNT_PREFIX_BYTES)) {
                    int[] annotationCounts = IndexUtils.countPerBitToObject(
                            cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    entry.getGtEntry(getGt(cell, ANNOTATION_SUMMARY_COUNT_PREFIX_BYTES))
                            .setAnnotationCounts(annotationCounts);
                } else if (columnStartsWith(cell, FILE_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, FILE_PREFIX_BYTES))
                            .setFileIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, FILE_DATA_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, FILE_DATA_PREFIX_BYTES))
                            .setFileDataIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, PARENTS_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, PARENTS_PREFIX_BYTES))
                            .setParentsIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, ANNOTATION_CT_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, ANNOTATION_CT_PREFIX_BYTES))
                            .setConsequenceTypeIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, ANNOTATION_BT_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, ANNOTATION_BT_PREFIX_BYTES))
                            .setBiotypeIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, ANNOTATION_TF_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, ANNOTATION_TF_PREFIX_BYTES))
                            .setTranscriptFlagIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, ANNOTATION_CT_BT_TF_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, ANNOTATION_CT_BT_TF_PREFIX_BYTES))
                            .setCtBtTfIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, ANNOTATION_POP_FREQ_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, ANNOTATION_POP_FREQ_PREFIX_BYTES))
                            .setPopulationFrequencyIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, ANNOTATION_CLINICAL_PREFIX_BYTES)) {
                    entry.getGtEntry(getGt(cell, ANNOTATION_CLINICAL_PREFIX_BYTES))
                            .setClinicalIndex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else if (columnStartsWith(cell, GENOTYPE_DISCREPANCY_COUNT_BYTES)) {
                    entry.setDiscrepancies(Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                } else {
                    throw new IllegalArgumentException("Unknown column '" + Bytes.toString(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength()) + "'");
                }
            } else {
                if (columnStartsWith(cell, MENDELIAN_ERROR_COLUMN_BYTES)) {
                    entry.setMendelianVariants(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                } else {
                    String gt = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    SampleIndexGtEntry gtEntry = entry.getGtEntry(gt);

                    gtEntry.setVariants(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        }

        return entry;
    }

    public static boolean columnStartsWith(Cell cell, byte[] prefix) {
        return AbstractPhoenixConverter.columnStartsWith(cell, prefix);
    }

    public Map<String, List<Variant>> convertToMap(Result result) {
        if (result == null) {
            return Collections.emptyMap();
        }
        Map<String, List<Variant>> map = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            if (SampleIndexSchema.isGenotypeColumn(cell)) {
                String gt = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                map.put(gt, converter.toVariants(cell));
            }
        }
        return map;
    }

    public Map<String, TreeSet<SampleIndexVariant>> convertToMapSampleVariantIndex(Result result) {
        if (result == null || result.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<Variant>> map = convertToMap(result);

        Map<String, TreeSet<SampleIndexVariant>> mapVariantFileIndex = new HashMap<>();
        SampleIndexVariant.SampleIndexVariantComparator comparator
                = new SampleIndexVariant.SampleIndexVariantComparator(schema);
        Map<String, BitInputStream> fileIndexMap = new HashMap<>();
        Map<String, ByteBuffer> fileDataMap = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            if (columnStartsWith(cell, FILE_PREFIX_BYTES)) {
                String gt = SampleIndexSchema.getGt(cell, FILE_PREFIX_BYTES);
                BitInputStream bis = new BitInputStream(
                        cell.getValueArray(),
                        cell.getValueOffset(),
                        cell.getValueLength());
                fileIndexMap.put(gt, bis);
            } else if (columnStartsWith(cell, FILE_DATA_PREFIX_BYTES)) {
                String gt = SampleIndexSchema.getGt(cell, FILE_DATA_PREFIX_BYTES);
                // Slice the buffer.
                // The wrap buffer contains the whole array, where the position is the offset.
                // The position might be set to 0 by `.reset()` method, which would allow reading data before offset.
                ByteBuffer byteBuffer = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()).slice();
                fileDataMap.put(gt, byteBuffer);
            }
        }
        for (Map.Entry<String, BitInputStream> entry : fileIndexMap.entrySet()) {
            String gt = entry.getKey();
            TreeSet<SampleIndexVariant> values = new TreeSet<>(comparator);
            mapVariantFileIndex.put(gt, values);

            BitInputStream fileIndexStream = entry.getValue();
            ByteBuffer fileDataBuffer = fileDataMap.get(gt);

            for (Variant variant : map.get(gt)) {
                BitBuffer fileIndexEntry;
                do {
                    fileIndexEntry = fileIndexSchema.readDocument(fileIndexStream);
                    ByteBuffer fileDataEntry;
                    if (fileDataBuffer == null) {
                        fileDataEntry = null;
                    } else {
                        fileDataEntry = fileDataSchema.readNextDocument(fileDataBuffer);
                    }
                    values.add(new SampleIndexVariant(variant, fileIndexEntry, fileDataEntry));
                } while (this.fileIndexSchema.isMultiFile(fileIndexEntry));
            }
        }
        return mapVariantFileIndex;
    }

    public int convertToCount(Result result) {
        int count = 0;
        for (Cell cell : result.rawCells()) {
            String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            if (column.startsWith(GENOTYPE_COUNT_PREFIX)) {
                count += Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }
        }
        return count;
    }
}
