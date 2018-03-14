package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;

import java.sql.Array;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 13/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantStatsCalculator extends AbstractPhoenixConverter implements Task<Result, VariantStats> {

    private final List<String> samples;
    private final StudyConfiguration sc;
//    private List<byte[]> rows;
    private HBaseToGenotypeCountConverter converter;

    public HBaseVariantStatsCalculator(byte[] columnFamily, StudyConfiguration sc, List<String> samples) {
        super(columnFamily);
        this.sc = sc;
        this.samples = samples;
        converter = new HBaseToGenotypeCountConverter(columnFamily);
    }

    @Override
    public void pre() throws Exception {

    }

    @Override
    public List<VariantStats> apply(List<Result> list) throws Exception {
        List<VariantStats> statsList = new ArrayList<>(list.size());
        for (Result result : list) {
            VariantStats stats = apply(result);
            statsList.add(stats);
        }
        return statsList;
    }

    public VariantStats apply(Result result) {
        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());

        Map<Genotype, Integer> gtCount = converter.apply(variant, result);
        VariantStats variantStats = new VariantStats(variant);
        VariantStatsCalculator.calculate(gtCount, variantStats);
        return variantStats;
    }

    private final class HBaseToGenotypeCountConverter extends HBaseToStudyEntryConverter {

        private final List<Integer> sampleIds;
        private final Set<Integer> sampleIdsSet;

        private HBaseToGenotypeCountConverter(byte[] columnFamily) {
            super(columnFamily, null, null);
            sampleIds = samples.stream().map(sc.getSampleIds()::get).collect(Collectors.toList());
            sampleIdsSet = samples.stream().map(sc.getSampleIds()::get).collect(Collectors.toSet());

            super.setSelectVariantElements(new VariantQueryUtils.SelectVariantElements(sc, sampleIds, Collections.emptyList()));
            super.setUnknownGenotype("./.");
        }

        public Map<Genotype, Integer> apply(Variant variant, Result result) {
            Set<Integer> procesedSamples = new HashSet<>();
            Map<String, Integer> gtStrCount = new HashMap<>(5);
            int fillMissingColumnValue = 0;
            for (Cell cell : result.rawCells()) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                if (endsWith(qualifier, VariantPhoenixHelper.SAMPLE_DATA_SUFIX_BYTES)) {
                    byte[] bytes = CellUtil.cloneValue(cell);
                    String columnName = Bytes.toString(qualifier);
                    String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                    //                Integer studyId = getStudyId(split);
                    Integer sampleId = getSampleId(split);
                    // Exclude other samples
                    if (sampleIdsSet.contains(sampleId)) {
                        procesedSamples.add(sampleId);

                        Array array = (Array) PVarcharArray.INSTANCE.toObject(bytes);
                        List<String> sampleData = toModifiableList(array, 0, 1);
                        addGt(gtStrCount, sampleData.get(0), 1);
                    }
                } else if (endsWith(qualifier, VariantPhoenixHelper.FILE_SUFIX_BYTES)) {
                    byte[] bytes = CellUtil.cloneValue(cell);
//                String columnName = Bytes.toString(qualifier);
//                String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
//                Integer studyId = getStudyId(split);
//                Integer fileId = getFileId(split);
                    Array array = (Array) PVarcharArray.INSTANCE.toObject(bytes);
                    List<String> fileData = toModifiableList(array, FILE_SEC_ALTS_IDX, FILE_SEC_ALTS_IDX + 1);
                    String secAlt = fileData.get(0);
                    if (StringUtils.isNotEmpty(secAlt)) {
                        logger.warn("WARN! found secAlt " + secAlt + " at variant " + variant);
                    }
                } else if (endsWith(qualifier, VariantPhoenixHelper.FILL_MISSING_SUFIX_BYTES)) {
                    byte[] bytes = CellUtil.cloneValue(cell);
                    fillMissingColumnValue = (Integer) PInteger.INSTANCE.toObject(bytes);
                }
            }

            // same order as "sampleIds"
            List<Boolean> missingUpdatedList = getMissingUpdatedSamples(sc, fillMissingColumnValue);

            String defaultGenotype = getDefaultGenotype(sc);

            if (procesedSamples.size() != sampleIds.size()) {
                int i = 0;
                int reference = 0;
                int missing = 0;
                for (Integer sampleId : sampleIds) {
                    if (!procesedSamples.contains(sampleId)) {
                        if (missingUpdatedList.get(i)) {
                            reference++;
                        } else {
                            missing++;
                        }
                    }
                    i++;
                }
                addGt(gtStrCount, "0/0", reference);
                addGt(gtStrCount, defaultGenotype, missing);
            }

            Map<Genotype, Integer> gtCountMap = new HashMap<>(gtStrCount.size());
            gtStrCount.forEach((str, count) -> gtCountMap.compute(new Genotype(str, variant.getReference(), variant.getAlternate()),
                    (key, value) -> value == null ? count : value + count));

            return gtCountMap;
        }

        private void addGt(Map<String, Integer> gtStrCount, String gt, int num) {
            gtStrCount.compute(gt, (key, value) -> value == null ? num : value + num);
        }
    }

}
