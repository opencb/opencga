package org.opencb.opencga.storage.hadoop.variant.gaps;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus.REFERENCE;
import static org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus.VARIANT;

/**
 * Created on 15/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsTask {

    private final StudyEntryToHBaseConverter studyConverter;
    private final StudyConfiguration studyConfiguration;
    private final Map<Integer, LinkedHashMap<String, Integer>> fileToSamplePositions = new HashMap<>();
    private final VariantMerger variantMerger;
    // fill-gaps-when-missing-gt
    private final boolean skipReferenceVariants;
    private final GenomeHelper helper;

    private Logger logger = LoggerFactory.getLogger(FillGapsTask.class);
    private boolean quiet = false;

    public FillGapsTask(StudyConfiguration studyConfiguration, GenomeHelper helper, boolean skipReferenceVariants) {
        this.studyConfiguration = studyConfiguration;

        this.skipReferenceVariants = skipReferenceVariants;

        this.helper = helper;
        studyConverter = new StudyEntryToHBaseConverter(this.helper.getColumnFamily(), studyConfiguration,
                true,
                Collections.singleton("?/?"), // Do not skip any genotype
                null); // Do not update release
        variantMerger = new VariantMerger(false).configure(studyConfiguration.getVariantHeader());
    }

    public FillGapsTask setQuiet(boolean quiet) {
        this.quiet = quiet;
        return this;
    }

    public VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put, List<Put> sampleIndexPuts,
                                             Integer fileId,
                                             VcfSliceProtos.VcfSlice nonRefVcfSlice, VcfSliceProtos.VcfSlice refVcfSlice) {
        return fillGaps(variant, missingSamples, put, sampleIndexPuts, fileId,
                nonRefVcfSlice, nonRefVcfSlice.getRecordsList().listIterator(),
                refVcfSlice, refVcfSlice.getRecordsList().listIterator());
    }

    public VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put, List<Put> sampleIndexPuts,
                                             Integer fileId,
                                             VcfSliceProtos.VcfSlice nonRefVcfSlice, ListIterator<VcfSliceProtos.VcfRecord> nonRefIterator,
                                             VcfSliceProtos.VcfSlice refVcfSlice, ListIterator<VcfSliceProtos.VcfRecord> refIterator) {
        final VariantOverlappingStatus overlappingStatus;

        // Three scenarios:
        //  Overlap with NO_VARIATION,
        //  Overlap with another variant
        //  No overlap

        List<Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord>> overlappingRecords = new ArrayList<>(1);
        if (nonRefVcfSlice != null) {
            boolean isVariantAlreadyLoaded = getOverlappingVariants(variant, fileId, nonRefVcfSlice, nonRefIterator, overlappingRecords);
            if (isVariantAlreadyLoaded) {
                return VariantOverlappingStatus.NONE;
            }
        }
        if (refVcfSlice != null) {
            boolean isVariantAlreadyLoaded = getOverlappingVariants(variant, fileId, refVcfSlice, refIterator, overlappingRecords);
            if (isVariantAlreadyLoaded) {
                String msg = "Found that the variant " + variant + " was already loaded in refVcfSlice!";
//                throw new IllegalStateException(msg);
                logger.warn(msg);
            }
        }

        VcfSliceProtos.VcfRecord vcfRecord;
        VcfSliceProtos.VcfSlice vcfSlice;
        if (overlappingRecords.isEmpty()) {
            // TODO: There was a gap in the gVCF?
            // May happen that the variant to fill is an insertion, and there is no overlapping
            // Should write "./." ? "0/0" for insertions?
            logger.debug("Not overlap for fileId " + fileId + " in variant " + variant);
            // Nothing to do!
            return null;
        } else if (overlappingRecords.size() > 1) {
            // TODO: What if multiple overlaps?

            // Discard ref_blocks
            List<Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord>> realVariants = overlappingRecords
                    .stream()
                    .filter(pair -> pair.getRight().getType() != VariantProto.VariantType.NO_VARIATION)
                    .collect(Collectors.toList());
            // If there is only one real variant, use it
            if (realVariants.size() == 1) {
                vcfRecord = realVariants.get(0).getRight();
                vcfSlice = realVariants.get(0).getLeft();
            } else {
                String msg = "Found multiple overlaps for variant " + variant + " in file " + fileId;
                if (!quiet) {
//                    throw new IllegalStateException(msg);
                    logger.warn(msg);
                }
                return null;
            }
        } else {
            vcfRecord = overlappingRecords.get(0).getRight();
            vcfSlice = overlappingRecords.get(0).getLeft();
        }
        Variant archiveVariant = convertToVariant(vcfSlice, vcfRecord, fileId);
        if (VcfRecordProtoToVariantConverter.getVariantType(vcfRecord.getType()).equals(VariantType.NO_VARIATION)) {
            FileEntry fileEntry = archiveVariant.getStudies().get(0).getFiles().get(0);
            fileEntry.getAttributes().remove(VCFConstants.END_KEY);
            if (StringUtils.isEmpty(fileEntry.getCall())) {
                fileEntry.setCall(archiveVariant.getStart() + ":" + archiveVariant.getReference() + ":.:0");
            }
            overlappingStatus = REFERENCE;
            studyConverter.convert(archiveVariant, put, missingSamples, overlappingStatus);
        } else {
            Variant mergedVariant = new Variant(
                    variant.getChromosome(),
                    variant.getStart(),
                    variant.getEnd(),
                    variant.getReference(),
                    variant.getAlternate());
            StudyEntry studyEntry = new StudyEntry();
            studyEntry.setFormat(archiveVariant.getStudies().get(0).getFormat());
            studyEntry.setSortedSamplesPosition(new LinkedHashMap<>());
            studyEntry.setSamplesData(new ArrayList<>());

            mergedVariant.addStudyEntry(studyEntry);
            mergedVariant.setType(variant.getType());

            mergedVariant = variantMerger.merge(mergedVariant, archiveVariant);
            overlappingStatus = VARIANT;

            if (studyEntry.getFormatPositions().containsKey("GT")) {
                int samplePosition = 0;
                Integer gtIdx = studyEntry.getFormatPositions().get("GT");
                for (String sampleName : studyEntry.getOrderedSamplesName()) {
                    Integer sampleId = studyConfiguration.getSampleIds().get(sampleName);
                    if (missingSamples.contains(sampleId)) {
                        String gt = studyEntry.getSamplesData().get(samplePosition).get(gtIdx);
                        // Only genotypes without the main alternate (0/2, 2/3, ...) should be written as pending.
                        if (SampleIndexDBLoader.validGenotype(gt) && !hasMainAlternate(gt)) {
                            Put sampleIndexPut = new Put(
                                    SampleIndexConverter.toRowKey(sampleId, variant.getChromosome(), variant.getStart()),
                                    put.getTimeStamp());
                            sampleIndexPut.addColumn(helper.getColumnFamily(), SampleIndexConverter.toPendingColumn(variant, gt), null);
                            sampleIndexPuts.add(sampleIndexPut);
                        }
                    }
                    samplePosition++;
                }
            }

            studyConverter.convert(mergedVariant, put, missingSamples, overlappingStatus);
        }
        return overlappingStatus;
    }

    protected boolean hasMainAlternate(String gt) {
        return StringUtils.contains(gt, '1');
    }

    public boolean getOverlappingVariants(Variant variant, int fileId,
                                          VcfSliceProtos.VcfSlice vcfSlice, ListIterator<VcfSliceProtos.VcfRecord> iterator,
                                          List<Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord>> overlappingRecords) {
        String chromosome = vcfSlice.getChromosome();
        int position = vcfSlice.getPosition();
        Integer resetPosition = null;
        boolean isAlreadyPresent = false;
        int firstIndex = iterator.nextIndex();
        // Assume sorted VcfRecords
        while (iterator.hasNext()) {
            VcfSliceProtos.VcfRecord vcfRecord = iterator.next();
            int start = VcfRecordProtoToVariantConverter.getStart(vcfRecord, position);
            int end = VcfRecordProtoToVariantConverter.getEnd(vcfRecord, position);
            String reference = vcfRecord.getReference();
            String alternate = vcfRecord.getAlternate();
            // If the VcfRecord starts after the variant, stop looking for variants
            if (isRegionAfterVariantStart(start, end, variant)) {
                if (resetPosition == null) {
                    resetPosition = Math.max(iterator.previousIndex() - 1, firstIndex);
                }
                // Shouldn't happen that the first VcfRecord from the iterator is beyond the variant to process,
                // and is not the first VcfRecord from the slice.
                // If so, there may be a bug, or the variants or the VcfSlice is not sorted
                if (firstIndex != 0 && firstIndex == iterator.previousIndex()) {
                    // This should never happen
                    throw new IllegalStateException("Variants or VcfSlice not in order!"
                            + " First next VcfRecord from iterator (index : " + firstIndex + ") "
                            + chromosome + ':' + start + '-' + end + ':' + reference + ':' + alternate
                            + " is after the current variant to process for file " + fileId
                    );
//                    // Something weird happened. Go back to the first position
//                    while (iterator.hasPrevious()) {
//                        iterator.previous();
//                    }
//                    firstIndex = 0;
                } else {
                    break;
                }
            } else if (overlapsWith(variant, chromosome, start, end)) {
                if (resetPosition == null) {
                    resetPosition = Math.max(iterator.previousIndex() - 1, firstIndex);
                }
                if (skipReferenceVariants && hasAllReferenceGenotype(vcfSlice, vcfRecord)) {
                    // Skip this variant
                    continue;
                }

                // If the same variant is present for this file in the VcfSlice, the variant is already loaded
                if (isVariantAlreadyLoaded(variant, vcfSlice, vcfRecord, chromosome, start, end, reference, alternate)) {
                    // Variant already loaded. Nothing to do!
                    isAlreadyPresent = true;
                    break;
                }

                overlappingRecords.add(ImmutablePair.of(vcfSlice, vcfRecord));
            }
        }
        // Send back the iterator
        if (resetPosition != null) {
//            logger.info("Reset from " + iterator.nextIndex() + " to " + resetPosition + ". fileId : " + fileId + " variant " + variant);
            while (iterator.nextIndex() > resetPosition) {
                iterator.previous();
            }
        }
        return isAlreadyPresent;
    }

    /**
     * Check if this VcfRecord is already loaded in the variant that is being processed.
     *
     * If so, the variant does not have a gap for this file. Nothing to do!
     */
    private static boolean isVariantAlreadyLoaded(Variant variant, VcfSliceProtos.VcfSlice slice, VcfSliceProtos.VcfRecord vcfRecord,
                                           String chromosome, int start, int end, String reference, String alternate) {
        // The variant is not loaded if is a NO_VARIATION (fast check first)
        if (vcfRecord.getType() == VariantProto.VariantType.NO_VARIATION) {
            return false;
        }
        // Check if the variant is the same
        if (!variant.sameGenomicVariant(new Variant(chromosome, start, end, reference, alternate))) {
            return false;
        }
        // If any of the genotypes is HOM_REF, the variant won't be completely loaded, so there may be a gap.
        return !hasAnyReferenceGenotype(slice, vcfRecord);
    }

    protected static boolean hasAnyReferenceGenotype(VcfSliceProtos.VcfSlice vcfSlice, VcfSliceProtos.VcfRecord vcfRecord) {
        for (VcfSliceProtos.VcfSample vcfSample : vcfRecord.getSamplesList()) {
            String gt = vcfSlice.getFields().getGts(vcfSample.getGtIndex());
            if (isHomRefDiploid(gt)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isHomRefDiploid(String gt) {
        return gt.equals("0/0") || gt.equals("0|0");
    }

    protected static boolean hasAllReferenceGenotype(VcfSliceProtos.VcfSlice vcfSlice, VcfSliceProtos.VcfRecord vcfRecord) {
        for (VcfSliceProtos.VcfSample vcfSample : vcfRecord.getSamplesList()) {
            String gt = vcfSlice.getFields().getGts(vcfSample.getGtIndex());
            if (!isHomRefDiploid(gt)) {
                return false;
            }
        }
        return true;
    }

    public Variant convertToVariant(VcfSliceProtos.VcfSlice vcfSlice, VcfSliceProtos.VcfRecord vcfRecord, Integer fileId) {
        VcfRecordProtoToVariantConverter converter = new VcfRecordProtoToVariantConverter(vcfSlice.getFields(),
                getSamplePosition(fileId), fileId.toString(), studyConfiguration.getStudyName());
        return converter.convert(vcfRecord, vcfSlice.getChromosome(), vcfSlice.getPosition());
    }

    public LinkedHashMap<String, Integer> getSamplePosition(Integer fileId) {
        return fileToSamplePositions.computeIfAbsent(fileId, missingFileId -> {
            LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(missingFileId)) {
                map.put(studyConfiguration.getSampleIds().inverse().get(sampleId), map.size());
            }
            return map;
        });
    }

    public static boolean isRegionAfterVariantStart(int start, int end, Variant variant) {
        int pos = Math.min(start, end);
        int variantPos = Math.min(variant.getStart(), variant.getEnd());
        return pos > variantPos;
    }

    public static boolean overlapsWith(Variant variant, String chromosome, int start, int end) {
//        return variant.overlapWith(chromosome, start, end, true);
        if (!StringUtils.equals(variant.getChromosome(), chromosome)) {
            return false; // Different Chromosome
        } else {
            return variant.getStart() <= end && variant.getEnd() >= start
                    // Insertions in the same position won't match previous statement.
                    || variant.getStart() == start && variant.getEnd() == end;
        }
    }

}
