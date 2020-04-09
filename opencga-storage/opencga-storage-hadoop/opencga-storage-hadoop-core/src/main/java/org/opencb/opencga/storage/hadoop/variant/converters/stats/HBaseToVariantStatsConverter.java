/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.converters.stats;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 07/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantStatsConverter extends AbstractPhoenixConverter {

    private final Logger logger = LoggerFactory.getLogger(HBaseToVariantStatsConverter.class);

    public HBaseToVariantStatsConverter() {
        super(GenomeHelper.COLUMN_FAMILY_BYTES);
    }
//
//    public static Converter<Result, List<VariantStats>> fromHbase(GenomeHelper genomeHelper) {
//        HBaseToVariantStatsConverter converter = new HBaseToVariantStatsConverter(genomeHelper);
//        return converter::convert;
//    }
//
//    public static Converter<ResultSet, List<VariantStats>> fromPhoenix(GenomeHelper genomeHelper) {
//        HBaseToVariantStatsConverter converter = new HBaseToVariantStatsConverter(genomeHelper);
//        return converter::convert;
//    }

    public Map<Integer, Map<Integer, VariantStats>> convert(Result result) {
//        String studyIdStr = String.valueOf(studyConfiguration.getStudyId());

        Map<Integer, Map<Integer, VariantStats>> studyCohortStatsMap = new HashMap<>();
        if (result.rawCells() != null) {
            for (Cell cell : result.rawCells()) {
                if (cell.getValueLength() != 0 && endsWith(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        VariantPhoenixHelper.COHORT_STATS_PROTOBUF_SUFFIX_BYTES)) {
                    byte[] value = CellUtil.cloneValue(cell);
                    String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                    Integer studyId = getStudyId(split);
                    Integer cohortId = getCohortId(split);

                    Map<Integer, VariantStats> statsMap = studyCohortStatsMap.computeIfAbsent(studyId, k -> new HashMap<>());
                    statsMap.put(cohortId, convert(value));
                }
            }
        }


        return studyCohortStatsMap;
    }

    public VariantStats convert(VariantRow.StatsColumn statsColumn) {
        return convert(statsColumn.toProto());
    }

    public Map<Integer, Map<Integer, VariantStats>> convert(ResultSet resultSet) {
//        String studyIdStr = String.valueOf(studyConfiguration.getStudyId());
        Map<Integer, Map<Integer, VariantStats>> studyCohortStatsMap = new HashMap<>();

        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                byte[] value = resultSet.getBytes(i);
                if (value != null && columnName.endsWith(VariantPhoenixHelper.COHORT_STATS_PROTOBUF_SUFFIX)) {
                    String[] split = columnName.split("_");
                    Integer studyId = getStudyId(split);
                    Integer cohortId = getCohortId(split);

                    Map<Integer, VariantStats> statsMap = studyCohortStatsMap.computeIfAbsent(studyId, k -> new HashMap<>());
                    statsMap.put(cohortId, convert(value));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return studyCohortStatsMap;
    }

    public Integer getStudyId(String[] split) {
        return Integer.valueOf(split[0]);
    }

    public Integer getCohortId(String[] split) {
        return Integer.valueOf(split[1]);
    }

    protected VariantStats convert(byte[] data) {
        try {
            return convert(VariantProto.VariantStats.parseFrom(data));
        } catch (InvalidProtocolBufferException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected VariantStats convert(VariantProto.VariantStats protoStats) {
        VariantStats stats = new VariantStats();

        stats.setSampleCount(protoStats.getSampleCount());
        stats.setFileCount(protoStats.getFileCount());
        stats.setMgf(protoStats.getMgf());
        if (!protoStats.getMgfGenotype().isEmpty()) {
            stats.setMgfGenotype(protoStats.getMgfGenotype());
        }
        stats.setMaf(protoStats.getMaf());
        String mafAllele = protoStats.getMafAllele();
        if (!mafAllele.isEmpty()) {
            // Proto does not allow null values.
            // proto | avro
            // ""    | null
            // "-"   | ""
            if (mafAllele.equals("-")) {
                stats.setMafAllele("");
            } else {
                stats.setMafAllele(mafAllele);
            }
        }
        stats.setAlleleCount(protoStats.getAlleleCount());
        stats.setAltAlleleCount(protoStats.getAltAlleleCount());
        stats.setAltAlleleFreq(protoStats.getAltAlleleFreq());
        stats.setRefAlleleCount(protoStats.getRefAlleleCount());
        stats.setRefAlleleFreq(protoStats.getRefAlleleFreq());
        stats.setMissingAlleleCount(protoStats.getMissingAlleleCount());
        stats.setMissingGenotypeCount(protoStats.getMissingGenotypeCount());
        stats.setQualityAvg(protoStats.getQualityAvg());
        stats.setQualityCount(protoStats.getQualityCount());

        stats.setGenotypeCount(protoStats.getGenotypeCountMap());
        stats.setGenotypeFreq(protoStats.getGenotypeFreqMap());

        stats.setFilterCount(protoStats.getFilterCountMap());
        stats.setFilterFreq(protoStats.getFilterFreqMap());

        return stats;
    }

}
