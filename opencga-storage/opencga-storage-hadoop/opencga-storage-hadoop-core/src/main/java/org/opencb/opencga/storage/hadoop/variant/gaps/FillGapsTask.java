package org.opencb.opencga.storage.hadoop.variant.gaps;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseToSampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus.*;

/**
 * Created on 15/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsTask {

    private final StudyEntryToHBaseConverter studyConverter;
    private final StudyMetadata studyMetadata;
    private final Map<Integer, LinkedHashMap<String, Integer>> fileToSamplePositions = new HashMap<>();
    private Map<String, Integer> sampleIdMap = new HashMap<>();
    private final VariantMerger variantMerger;
    // fill-gaps-when-missing-gt
    private final boolean skipReferenceVariants;
    private final GenomeHelper helper;
    private final VariantStorageMetadataManager metadataManager;

    private Logger logger = LoggerFactory.getLogger(FillGapsTask.class);
    private boolean quiet = false;

    public FillGapsTask(StudyMetadata studyMetadata, GenomeHelper helper, boolean skipReferenceVariants,
                        VariantStorageMetadataManager metadataManager) {
        this.studyMetadata = studyMetadata;
        this.skipReferenceVariants = skipReferenceVariants;

        this.helper = helper;
        this.metadataManager = metadataManager;
        studyConverter = new StudyEntryToHBaseConverter(this.helper.getColumnFamily(), studyMetadata.getId(), metadataManager,
                true,
                null, // Do not update release
                true); // Do not skip any genotype
        variantMerger = new VariantMerger(false).configure(studyMetadata.getVariantHeader());
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

        final VcfSliceProtos.VcfRecord vcfRecord;
        final VcfSliceProtos.VcfSlice vcfSlice;
        if (overlappingRecords.isEmpty()) {
            if (skipReferenceVariants) {
                // We are not reading reference blocks, so there gaps are expected and read as HOM_REF
                return null;
            } else {
                // There was a gap in the gVCF?
//                logger.debug("Not overlap for fileId " + fileId + " in variant " + variant);

                if (variant.getType().equals(VariantType.INDEL) && variant.getReference().isEmpty()) {
                    // May happen that the variant to fill is an insertion, and there is no overlapping
                    // Write HOM_REF genotype for this samples
                    return processVariantFileGap(variant, missingSamples, put, fileId, "0/0");
                } else {
                    // There was a gap in the original file
                    return processVariantFileGap(variant, missingSamples, put, fileId, GenotypeClass.UNKNOWN_GENOTYPE);
                }
            }
        } else if (overlappingRecords.size() > 1) {
            // Discard ref_blocks
            List<Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord>> realVariants = overlappingRecords
                    .stream()
                    .filter(pair -> pair.getRight().getType() != VariantProto.VariantType.NO_VARIATION)
                    .collect(Collectors.toList());
            if (realVariants.size() > 1) {
                // Check if all the variants are different versions of the same multi-allelic variant
                Set<String> calls = new HashSet<>();
                for (Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord> pair : realVariants) {
                    String call = pair.getValue().getCall();
                    if (call.isEmpty()) {
                        calls.add(null);
                    } else {
                        calls.add(call.substring(0, call.lastIndexOf(':')));
                    }
                }
                // All the variants are from the same call. Select any.
                if (calls.size() == 1 && !calls.contains(null)) {
                    realVariants = Collections.singletonList(realVariants.stream()
                            .min(Comparator.comparing(pair -> pair.getValue().getCall())).get());
                }
            }
            // If there is only one real variant, use it
            if (realVariants.size() == 1) {
                vcfRecord = realVariants.get(0).getRight();
                vcfSlice = realVariants.get(0).getLeft();
            } else {
//                String msg = "Found multiple overlaps for variant " + variant + " in file " + fileId;
//                if (!quiet) {
////                    throw new IllegalStateException(msg);
//                    logger.warn(msg);
//                }
                return processMultipleOverlappings(variant, missingSamples, put, sampleIndexPuts, fileId);
            }
        } else {
            vcfRecord = overlappingRecords.get(0).getRight();
            vcfSlice = overlappingRecords.get(0).getLeft();
        }
        Variant archiveVariant = convertToVariant(vcfSlice, vcfRecord, fileId);

        if (archiveVariant.getType().equals(VariantType.NO_VARIATION)) {
            overlappingStatus = processReferenceOverlap(missingSamples, put, variant, archiveVariant);
        } else {
            overlappingStatus = processVariantOverlap(variant, missingSamples, put, sampleIndexPuts, archiveVariant);
        }
        return overlappingStatus;
    }

    protected VariantOverlappingStatus processReferenceOverlap(Set<Integer> missingSamples, Put put,
                                                               Variant variant, Variant archiveVariant) {
        VariantOverlappingStatus overlappingStatus = REFERENCE;

        FileEntry fileEntry = archiveVariant.getStudies().get(0).getFiles().get(0);
        fileEntry.getAttributes().remove(VCFConstants.END_KEY);
        if (StringUtils.isEmpty(fileEntry.getCall())) {
            fileEntry.setCall(archiveVariant.getStart() + ":" + archiveVariant.getReference() + ":" + archiveVariant.getAlternate() + ":0");
        }
        // SYMBOLIC reference overlap -- <*> , <NON_REF>
        if (VariantType.NO_VARIATION.equals(archiveVariant.getType())
                && !archiveVariant.getAlternate().isEmpty()
                && !archiveVariant.getAlternate().equals(Allele.NO_CALL_STRING)) {

            // Create template variant
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

            // Merge NO_VARIATION into the template variant
            mergedVariant = variantMerger.merge(mergedVariant, archiveVariant);

            // Convert study information to PUT
            studyConverter.convert(mergedVariant, put, missingSamples, overlappingStatus);

        } else {
            studyConverter.convert(archiveVariant, put, missingSamples, overlappingStatus);
        }
        return overlappingStatus;
    }

    protected VariantOverlappingStatus processVariantOverlap(Variant variant, Set<Integer> missingSamples, Put put,
                                                             List<Put> sampleIndexPuts, Variant archiveVariant) {
        VariantOverlappingStatus overlappingStatus = VARIANT;

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


        if (studyEntry.getFormatPositions().containsKey("GT")) {
            int samplePosition = 0;
            Integer gtIdx = studyEntry.getFormatPositions().get("GT");
            for (String sampleName : studyEntry.getOrderedSamplesName()) {
                Integer sampleId = getSampleId(sampleName);
                if (missingSamples.contains(sampleId)) {
                    String gt = studyEntry.getSamplesData().get(samplePosition).get(gtIdx);
                    // Only genotypes without the main alternate (0/2, 2/3, ...) should be written as pending.
                    if (SampleIndexDBLoader.validGenotype(gt) && !hasMainAlternate(gt)) {
                        Put sampleIndexPut = buildSampleIndexPut(variant, put, sampleId, gt);
                        sampleIndexPuts.add(sampleIndexPut);
                    }
                }
                samplePosition++;
            }
        }

        studyConverter.convert(mergedVariant, put, missingSamples, overlappingStatus);
        return overlappingStatus;
    }

    protected VariantOverlappingStatus processVariantFileGap(Variant variant, Set<Integer> missingSamples, Put put, Integer fileId,

                                                             String gt) {
        return processVariantFile(variant, missingSamples, put, fileId, GAP, gt);
    }

    private VariantOverlappingStatus processVariantFile(Variant variant, Set<Integer> missingSamples, Put put, Integer fileId,
                                                        VariantOverlappingStatus overlappingStatus, String gt) {
        LinkedHashMap<String, Integer> samplePosition = getSamplePosition(fileId);
        List<List<String>> samplesData = new ArrayList<>(samplePosition.size());
        for (int i = 0; i < samplePosition.size(); i++) {
            samplesData.add(Collections.singletonList(gt));
        }

        VariantBuilder builder = Variant.newBuilder(
                variant.getChromosome(),
                variant.getStart(),
                variant.getEnd(),
                variant.getReference(),
                variant.getAlternate())
                .setStudyId(String.valueOf(studyMetadata.getId()))
                .setFormat("GT")
                .setFileId(fileId.toString())
                .setSamplesPosition(samplePosition)
                .setSamplesData(samplesData);

        studyConverter.convert(builder.build(), put, missingSamples, overlappingStatus);
        return overlappingStatus;
    }

    protected VariantOverlappingStatus processMultipleOverlappings(Variant variant, Set<Integer> missingSamples, Put put,
                                                                   List<Put> sampleIndexPuts, Integer fileId) {
        VariantOverlappingStatus overlappingStatus = MULTI;

        String gt = "2/2";
        LinkedHashMap<String, Integer> samplePosition = getSamplePosition(fileId);
        List<List<String>> samplesData = new ArrayList<>(samplePosition.size());
        for (int i = 0; i < samplePosition.size(); i++) {
            samplesData.add(Collections.singletonList(gt));
        }

        for (Integer sampleId : missingSamples) {
            Put sampleIndexPut = buildSampleIndexPut(variant, put, sampleId, gt);
            sampleIndexPuts.add(sampleIndexPut);
        }

        VariantBuilder builder = Variant.newBuilder(
                    variant.getChromosome(),
                    variant.getStart(),
                    variant.getEnd(),
                    variant.getReference(),
                    variant.getAlternate())
                .addAlternate("<*>")
                .setStudyId(String.valueOf(studyMetadata.getId()))
                .setFileId(fileId.toString())
                // add overlapping variants at attributes
                .setFormat("GT")
                .setSamplesPosition(samplePosition)
                .setSamplesData(samplesData);


//        processVariantOverlap(variant, missingSamples, put, sampleIndexPuts, builder.build());
        studyConverter.convert(builder.build(), put, missingSamples, overlappingStatus);

        return overlappingStatus;
    }

    private Put buildSampleIndexPut(Variant variant, Put put, Integer sampleId, String gt) {
        Put sampleIndexPut = new Put(
                HBaseToSampleIndexConverter.toRowKey(sampleId, variant.getChromosome(), variant.getStart()),
                put.getTimeStamp());
        sampleIndexPut.addColumn(helper.getColumnFamily(), HBaseToSampleIndexConverter.toPendingColumn(variant, gt), null);
        return sampleIndexPut;
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
            if (overlapsWith(variant, chromosome, start, end)) {
                if (resetPosition == null) {
                    resetPosition = Math.max(iterator.previousIndex() - 1, firstIndex);
                }

//                if (skipReferenceVariants && hasAllReferenceGenotype(vcfSlice, vcfRecord)) {
//                    // Skip this variant
//                    continue;
//                }

                // If the same variant is present for this file in the VcfSlice, the variant is already loaded
                if (isVariantAlreadyLoaded(variant, vcfSlice, vcfRecord, chromosome, start, end, reference, alternate)) {
                    // Variant already loaded. Nothing to do!
                    isAlreadyPresent = true;
                    break;
                }

                overlappingRecords.add(ImmutablePair.of(vcfSlice, vcfRecord));
            } else if (isRegionAfterVariantStart(start, end, variant)) {
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
            }
        }
        if (resetPosition == null && !iterator.hasNext()) {
            // If the iterator reaches the end without finding any point, reset the iterator
            resetPosition = firstIndex;
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

    protected Variant convertToVariant(VcfSliceProtos.VcfSlice vcfSlice, VcfSliceProtos.VcfRecord vcfRecord, Integer fileId) {
        VcfRecordProtoToVariantConverter converter = new VcfRecordProtoToVariantConverter(vcfSlice.getFields(),
                getSamplePosition(fileId), fileId.toString(), studyMetadata.getName());
        return converter.convert(vcfRecord, vcfSlice.getChromosome(), vcfSlice.getPosition());
    }

    protected Integer getSampleId(String sampleName) {
        return sampleIdMap.computeIfAbsent(sampleName, s -> metadataManager.getSampleId(studyMetadata.getId(), sampleName));
    }

    protected LinkedHashMap<String, Integer> getSamplePosition(Integer fileId) {
        return fileToSamplePositions.computeIfAbsent(fileId, missingFileId -> {
            LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
            for (Integer sampleId : metadataManager.getFileMetadata(studyMetadata.getId(), missingFileId).getSamples()) {
                map.put(metadataManager.getSampleName(studyMetadata.getId(), sampleId), map.size());
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
