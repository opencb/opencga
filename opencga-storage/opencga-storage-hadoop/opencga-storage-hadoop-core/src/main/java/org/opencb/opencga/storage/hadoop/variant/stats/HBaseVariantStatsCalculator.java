package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.merge.VariantAlternateRearranger;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryFields;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.feature.Genotype.HOM_REF;

/**
 * Created on 13/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantStatsCalculator extends AbstractPhoenixConverter implements Task<VariantRow, VariantStats> {

    private final List<Integer> sampleIds;
    private final StudyMetadata sm;
    private final boolean statsMultiAllelic;
    //    private List<byte[]> rows;
    private HBaseToGenotypeCountConverter converter;

    public HBaseVariantStatsCalculator(byte[] columnFamily, VariantStorageMetadataManager metadataManager, StudyMetadata sm,
                                       List<Integer> sampleIds, boolean statsMultiAllelic, String unknownGenotype) {
        super(columnFamily);
        this.sm = sm;
        this.sampleIds = sampleIds;
        this.statsMultiAllelic = statsMultiAllelic;
        converter = new HBaseToGenotypeCountConverter(metadataManager, columnFamily, statsMultiAllelic, unknownGenotype);
    }

    @Override
    public List<VariantStats> apply(List<VariantRow> list) throws Exception {
        return list.stream().map(this::apply).collect(Collectors.toCollection(() -> new ArrayList<>(list.size())));
    }

    public VariantStats apply(Result result) {
        return apply(new VariantRow(result));
    }

    public VariantStats apply(VariantRow result) {
        Variant variant = result.getVariant();

        Map<Genotype, Integer> gtCount = convert(result, variant, null);

        return calculate(variant, gtCount);
    }

    protected Map<Genotype, Integer> convert(Result result, Variant variant, Map<Genotype, Integer> gtCount) {
        return convert(new VariantRow(result), variant, gtCount);
    }

    protected Map<Genotype, Integer> convert(VariantRow result, Variant variant, Map<Genotype, Integer> gtCount) {
        Map<Genotype, Integer> newGtCount = converter.apply(variant, result);

        if (gtCount != null) {
            gtCount.forEach((gt, count) -> newGtCount.merge(gt, count, Integer::sum));
        }
        return newGtCount;
    }

    protected VariantStats calculate(Variant variant, Map<Genotype, Integer> gtCount) {
        return VariantStatsCalculator.calculate(variant, gtCount, statsMultiAllelic);
    }

    private final class HBaseToGenotypeCountConverter extends HBaseToStudyEntryConverter {
        private final Set<Integer> sampleIdsSet;
        private final Set<Integer> fileIds;
        private final Map<Integer, Collection<Integer>> samplesInFile;

        private HBaseToGenotypeCountConverter(VariantStorageMetadataManager metadataManager, byte[] columnFamily,
                                              boolean statsMultiAllelic, String unknownGenotype) {
            super(columnFamily, metadataManager, null);
            sampleIdsSet = new HashSet<>(sampleIds);
            if (excludeFiles(statsMultiAllelic, unknownGenotype)) {
                fileIds = Collections.emptySet();
                samplesInFile = Collections.emptyMap();
            } else {
                fileIds = new HashSet<>(sampleIds.size());
                samplesInFile = new HashMap<>(sampleIds.size());

                metadataManager.sampleMetadataIterator(sm.getId()).forEachRemaining(sampleMetadata -> {
                    int sampleId = sampleMetadata.getId();
                    if (sampleIds.contains(sampleId)) {
                        fileIds.addAll(sampleMetadata.getFiles());
                        for (Integer file : sampleMetadata.getFiles()) {
                            samplesInFile.computeIfAbsent(file, f -> new HashSet<>()).add(sampleId);
                        }
                    }
                });
            }

            super.setSelectVariantElements(new VariantQueryFields(sm, sampleIds, Collections.emptyList()));
            super.setUnknownGenotype(unknownGenotype);
        }

        public Map<Genotype, Integer> apply(Variant variant, VariantRow result) {
            Set<Integer> processedSamples = new HashSet<>();
            Set<Integer> filesInThisVariant = new HashSet<>();
            AtomicInteger fillMissingColumnValue = new AtomicInteger(-1);
            Map<Integer, String> sampleToGT = new HashMap<>();
            Map<String, List<Integer>> alternateFileMap = new HashMap<>();

            result.walker()
                    .onSample(sample -> {
                        int sampleId = sample.getSampleId();
                        // Exclude other samples
                        if (sampleIdsSet.contains(sampleId)) {
                            processedSamples.add(sampleId);

                            String gt = sample.getGT();
                            if (gt.isEmpty()) {
                                // This is a really weird situation, most likely due to errors in the input files
                                logger.error("Empty genotype at sample " + sampleId + " in variant " + variant);
                            } else {
                                sampleToGT.put(sampleId, gt);
                            }
                        }
                    })
                    .onFile(file -> {
                        int fileId = file.getFileId();

                        if (fileIds.contains(fileId)) {
                            filesInThisVariant.add(fileId);

                            String secAlt = file.getString(FILE_SEC_ALTS_IDX);

                            if (StringUtils.isNotEmpty(secAlt)) {
                                alternateFileMap.computeIfAbsent(secAlt, (key) -> new ArrayList<>()).add(fileId);
                            }
                        }
                    })
                    .onFillMissing(fillMissingColumnValue::set)
                    .walk();

            // If there are multiple different alternates, rearrange genotype
            if (alternateFileMap.size() > 1) {
                rearrangeGenotypes(variant, sampleToGT, alternateFileMap);
            }

            Map<String, Integer> gtStrCount = new HashMap<>(5);
            for (String gt : sampleToGT.values()) {
                addGt(gtStrCount, gt, 1);
            }

            if (processedSamples.size() != sampleIds.size()) {
                String defaultGenotype = getDefaultGenotype(sm);

                if (defaultGenotype.equals(HOM_REF)) {
                    // All missing samples are reference.
                    addGt(gtStrCount, HOM_REF, sampleIds.size() - processedSamples.size());
                } else if (fillMissingColumnValue.get() == -1 && filesInThisVariant.isEmpty()) {
                    // All missing samples are unknown.
                    addGt(gtStrCount, defaultGenotype, sampleIds.size() - processedSamples.size());
                } else {
                    // Some samples are missing, some other are reference.

                    // Same order as "sampleIds"
                    List<Boolean> missingUpdatedList = getMissingUpdatedSamples(sm, fillMissingColumnValue.get());
                    List<Boolean> sampleWithVariant = getSampleWithVariant(sm, filesInThisVariant);
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
                    addGt(gtStrCount, HOM_REF, reference);
                    addGt(gtStrCount, defaultGenotype, missing);
                }
            }

            Map<Genotype, Integer> gtCountMap = new HashMap<>(gtStrCount.size());
            gtStrCount.forEach((str, count) -> gtCountMap.merge(new Genotype(str), count, Integer::sum));

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
                    for (Integer sampleId : samplesInFile.get(fileId)) {
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
            gtStrCount.merge(gt, num, Integer::sum);
        }
    }

    protected static boolean excludeFiles(boolean statsMultiAllelic, String unknownGenotype) {
        return !statsMultiAllelic && unknownGenotype.equals(HOM_REF);
    }

}
