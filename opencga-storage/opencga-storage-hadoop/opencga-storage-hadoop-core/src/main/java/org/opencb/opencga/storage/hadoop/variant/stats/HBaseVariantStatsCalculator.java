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
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.merge.VariantAlternateRearranger;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
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

        return VariantStatsCalculator.calculate(variant, gtCount);
    }

    private final class HBaseToGenotypeCountConverter extends HBaseToStudyEntryConverter {

        private final List<Integer> sampleIds;
        private final Set<Integer> sampleIdsSet;
        private final Set<Integer> fileIds;

        private HBaseToGenotypeCountConverter(byte[] columnFamily) {
            super(columnFamily, null, null);
            sampleIds = samples.stream().map(sc.getSampleIds()::get).collect(Collectors.toList());
            sampleIdsSet = samples.stream().map(sc.getSampleIds()::get).collect(Collectors.toSet());

            fileIds = StudyConfigurationManager.getFileIdsFromSampleIds(sc, sampleIds);
            super.setSelectVariantElements(new VariantQueryUtils.SelectVariantElements(sc, sampleIds, Collections.emptyList()));
            super.setUnknownGenotype("./.");
        }

        public Map<Genotype, Integer> apply(Variant variant, Result result) {
            Set<Integer> processedSamples = new HashSet<>();
            Set<Integer> filesInThisVariant = new HashSet<>();
            int fillMissingColumnValue = -1;
            Map<Integer, String> sampleToGT = new HashMap<>();
            Map<String, List<Integer>> alternateFileMap = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                if (endsWith(qualifier, VariantPhoenixHelper.SAMPLE_DATA_SUFIX_BYTES)) {
                    byte[] value = CellUtil.cloneValue(cell);
                    String columnName = Bytes.toString(qualifier);
                    String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                    //                Integer studyId = getStudyId(split);
                    Integer sampleId = getSampleId(split);
                    // Exclude other samples
                    if (sampleIdsSet.contains(sampleId)) {
                        processedSamples.add(sampleId);

                        Array array = (Array) PVarcharArray.INSTANCE.toObject(value);
                        List<String> sampleData = toModifiableList(array, 0, 1);
                        sampleToGT.put(sampleId, sampleData.get(0));
                    }
                } else if (endsWith(qualifier, VariantPhoenixHelper.FILE_SUFIX_BYTES)) {
                    byte[] value = CellUtil.cloneValue(cell);
                    String columnName = Bytes.toString(qualifier);
                    String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
//                    Integer studyId = getStudyId(split);
                    Integer fileId = Integer.valueOf(getFileId(split));
                    if (fileIds.contains(fileId)) {
                        filesInThisVariant.add(fileId);
                        Array array = (Array) PVarcharArray.INSTANCE.toObject(value);
                        List<String> fileData = toModifiableList(array, FILE_SEC_ALTS_IDX, FILE_SEC_ALTS_IDX + 1);
                        String secAlt = fileData.get(0);
                        if (StringUtils.isNotEmpty(secAlt)) {
                            alternateFileMap.computeIfAbsent(secAlt, (key) -> new ArrayList<>()).add(fileId);
                        }
                    }
                } else if (endsWith(qualifier, VariantPhoenixHelper.FILL_MISSING_SUFIX_BYTES)) {
                    byte[] value = CellUtil.cloneValue(cell);
                    fillMissingColumnValue = (Integer) PInteger.INSTANCE.toObject(value);
                }
            }

            // If there are multiple different alternates, rearrange genotype
            if (alternateFileMap.size() > 1) {
                rearrangeGenotypes(variant, sampleToGT, alternateFileMap);
            }

            Map<String, Integer> gtStrCount = new HashMap<>(5);
            for (String gt : sampleToGT.values()) {
                addGt(gtStrCount, gt, 1);
            }

            if (processedSamples.size() != sampleIds.size()) {
                String defaultGenotype = getDefaultGenotype(sc);

                if (defaultGenotype.equals("0/0")) {
                    // All missing samples are reference.
                    addGt(gtStrCount, "0/0", sampleIds.size() - processedSamples.size());
                } else if (fillMissingColumnValue == -1 && filesInThisVariant.isEmpty()) {
                    // All missing samples are unknown.
                    addGt(gtStrCount, defaultGenotype, sampleIds.size() - processedSamples.size());
                } else {
                    // Some samples are missing, some other are reference.

                    // Same order as "sampleIds"
                    List<Boolean> missingUpdatedList = getMissingUpdatedSamples(sc, fillMissingColumnValue);
                    List<Boolean> sampleWithVariant = getSampleWithVariant(sc, filesInThisVariant);
                    int i = 0;
                    int reference = 0;
                    int missing = 0;
                    for (Integer sampleId : sampleIds) {
                        if (!processedSamples.contains(sampleId)) {
                            if (missingUpdatedList.get(i) || sampleWithVariant.get(i)) {
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
            }

            Map<Genotype, Integer> gtCountMap = new HashMap<>(gtStrCount.size());
            gtStrCount.forEach((str, count) -> gtCountMap.compute(new Genotype(str, variant.getReference(), variant.getAlternate()),
                    (key, value) -> value == null ? count : value + count));

            return gtCountMap;
        }

        private void rearrangeGenotypes(Variant variant, Map<Integer, String> sampleToGT, Map<String, List<Integer>> alternateFileMap) {
            // Get set of reordered alternates.
            // Include the main alternate as first alternate. The "alternateFileMap" only contains the secondary alternates.
            Set<AlternateCoordinate> reorderedAlternatesSet = new LinkedHashSet<>();
            AlternateCoordinate mainAlternate = new AlternateCoordinate(
                    variant.getChromosome(), variant.getStart(), variant.getEnd(),
                    variant.getReference(), variant.getAlternate(), variant.getType());
            reorderedAlternatesSet.add(mainAlternate);

            // Add other secondary alternates
            for (Map.Entry<String, List<Integer>> entry : alternateFileMap.entrySet()) {
                String secAlt = entry.getKey();
                List<AlternateCoordinate> alternateCoordinates = getAlternateCoordinates(secAlt);
                reorderedAlternatesSet.addAll(alternateCoordinates);
            }
            List<AlternateCoordinate> reorderedAlternates = new ArrayList<>(reorderedAlternatesSet);

            boolean first = true;
            for (Map.Entry<String, List<Integer>> entry : alternateFileMap.entrySet()) {
                if (first) {
                    first = false;
                    // Skip first alternate. As it is the first, it does not need to be rearranged.
                    continue;
                }
                String secAlt = entry.getKey();
                List<AlternateCoordinate> alternateCoordinates = getAlternateCoordinates(secAlt);
                // Same as before. Add the main alternate as first alternate. It only contains secondary alternates.
                alternateCoordinates.add(0, mainAlternate);
                VariantAlternateRearranger rearranger = new VariantAlternateRearranger(alternateCoordinates, reorderedAlternates);

                for (Integer fileId : entry.getValue()) {
                    for (Integer sampleId : sc.getSamplesInFiles().get(fileId)) {
                        String gt = sampleToGT.get(sampleId);
                        if (gt != null) {
                            try {
                                Genotype newGt = rearranger.rearrangeGenotype(new Genotype(gt));
                                sampleToGT.put(sampleId, newGt.toString());
                            } catch (RuntimeException e) {
                                throw new IllegalStateException("Error rearranging GT " + gt + " at variant " + variant
                                        + " with reorderedAlternates " + reorderedAlternates
                                        + " and originalAlternates " + alternateCoordinates, e);
                            }
                        }
                    }
                }
            }
        }

        private void addGt(Map<String, Integer> gtStrCount, String gt, int num) {
            gtStrCount.compute(gt, (key, value) -> value == null ? num : value + num);
        }
    }

}
