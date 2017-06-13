package org.opencb.opencga.storage.hadoop.variant.converters.samples;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Created on 26/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToSamplesDataConverter extends AbstractPhoenixConverter {

    public HBaseToSamplesDataConverter(GenomeHelper genomeHelper) {
        super(genomeHelper.getColumnFamily());
    }

    public Map<Integer, Map<Integer, List<String>>> convert(ResultSet resultSet) {
        Map<Integer, Map<Integer, List<String>>> samplesData = new HashMap<>();
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                if (columnName.endsWith(VariantPhoenixHelper.SAMPLE_DATA_SUFIX)) {
                    Array value = resultSet.getArray(i);
                    if (value != null) {
                        String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                        Integer studyId = getStudyId(split);
                        Integer sampleId = getSampleId(split);

                        Map<Integer, List<String>> studyMap = samplesData.computeIfAbsent(studyId, k -> new HashMap<>());
                        studyMap.put(sampleId, toList(value));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return samplesData;
    }

    public Map<Integer, Map<Integer, List<String>>> convert(Result result) {
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(columnFamily);
        Map<Integer, Map<Integer, List<String>>> samplesData = new HashMap<>();

        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            byte[] columnBytes = entry.getKey();
            byte[] value = entry.getValue();
            if (value != null && endsWith(columnBytes, VariantPhoenixHelper.SAMPLE_DATA_SUFIX_BYTES)) {
                String columnName = Bytes.toString(columnBytes);
                String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                Integer studyId = getStudyId(split);
                Integer sampleId = getSampleId(split);

                Array array = (Array) PVarcharArray.INSTANCE.toObject(value);
                List<String> sampleData = toList(array);

                Map<Integer, List<String>> studyMap = samplesData.computeIfAbsent(studyId, k -> new HashMap<>());
                studyMap.put(sampleId, sampleData);
            }
        }

        return samplesData;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> toList(Array value) {
        try {
            return Arrays.asList((T[]) value.getArray());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private Integer getStudyId(String[] split) {
        return Integer.valueOf(split[0]);
    }

    private Integer getSampleId(String[] split) {
        return Integer.valueOf(split[1]);
    }

}
