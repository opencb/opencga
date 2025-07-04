package org.opencb.opencga.storage.hadoop.variant.gaps;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryMultiFileToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus.*;

/**
 * Created on 15/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsTask {

    private final StudyEntryToHBaseConverter studyConverter;
    protected final StudyMetadata studyMetadata;
    private final Map<Integer, LinkedHashMap<String, Integer>> fileToSamplePositions = new HashMap<>();
    private Map<String, Integer> sampleIdMap = new HashMap<>();
    private final VariantMerger variantMerger;
    // fill-gaps-when-missing-gt
    private final boolean skipReferenceVariants;
    private final boolean simplifiedNewMultiAllelicVariants;
    private final Set<String> newGenotypes = new HashSet<>();
    private final VariantStorageMetadataManager metadataManager;

    private Logger logger = LoggerFactory.getLogger(FillGapsTask.class);
    private final String gapsGenotype;

    public FillGapsTask(VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata, boolean skipReferenceVariants,
                        boolean simplifiedNewMultiAllelicVariants, String gapsGenotype) {
        this.studyMetadata = studyMetadata;
        this.skipReferenceVariants = skipReferenceVariants;

        this.metadataManager = metadataManager;
        studyConverter = new StudyEntryMultiFileToHBaseConverter(GenomeHelper.COLUMN_FAMILY_BYTES, studyMetadata.getId(), metadataManager,
                true,
                null, // Do not update release
                true, // Do not skip any genotype
                false);
        variantMerger = new VariantMerger(false).configure(studyMetadata.getVariantHeader());
        this.simplifiedNewMultiAllelicVariants = simplifiedNewMultiAllelicVariants;
        this.gapsGenotype = gapsGenotype;
    }

    public VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put,
                                             Integer fileId,
                                             VcfSliceProtos.VcfSlice nonRefVcfSlice, VcfSliceProtos.VcfSlice refVcfSlice) {
        return fillGaps(variant, missingSamples, put, fileId,
                FillGapsRecordVcfSlice.listIterator(nonRefVcfSlice),
                FillGapsRecordVcfSlice.listIterator(refVcfSlice));
    }

    public VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put,
                                             Integer fileId,
                                             VcfSliceProtos.VcfSlice nonRefVcfSlice, ListIterator<VcfSliceProtos.VcfRecord> nonRefIterator,
                                             VcfSliceProtos.VcfSlice refVcfSlice, ListIterator<VcfSliceProtos.VcfRecord> refIterator) {
        return fillGaps(variant, missingSamples, put, fileId,
                FillGapsRecordVcfSlice.listIterator(nonRefVcfSlice, nonRefIterator),
                FillGapsRecordVcfSlice.listIterator(refVcfSlice, refIterator));
    }

    public VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put,
                                             Integer fileId,
                                             ListIterator<Variant> nonRefVariants) {
        return fillGaps(variant, missingSamples, put, fileId,
                FillGapsRecordVariant.listIterator(nonRefVariants),
                null);
    }

    protected VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put,
                Integer fileId,
                ListIterator<FillGapsRecord> nonRefIterator,
                ListIterator<FillGapsRecord> refIterator) {
        final VariantOverlappingStatus overlappingStatus;

        // Three scenarios:
        //  Overlap with NO_VARIATION,
        //  Overlap with another variant
        //  No overlap

        List<FillGapsRecord> overlappingRecords = new ArrayList<>(1);
        if (nonRefIterator != null) {
            boolean isVariantAlreadyLoaded = getOverlappingVariants(variant, fileId, nonRefIterator, overlappingRecords);
            if (isVariantAlreadyLoaded) {
                return VariantOverlappingStatus.NONE;
            }
        }
        if (refIterator != null) {
            boolean isVariantAlreadyLoaded = getOverlappingVariants(variant, fileId, refIterator, overlappingRecords);
            if (isVariantAlreadyLoaded) {
                String msg = "Found that the variant " + variant + " was already loaded in refVcfSlice!";
//                throw new IllegalStateException(msg);
                logger.warn(msg);
            }
        }

        final FillGapsRecord record;
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
                    return processVariantFileGap(variant, missingSamples, put, fileId, gapsGenotype);
                }
            }
        } else if (overlappingRecords.size() > 1) {
            // Discard ref_blocks
            List<FillGapsRecord> realVariants = overlappingRecords
                    .stream()
                    .filter(r -> !r.isNoVariant())
                    .collect(Collectors.toList());
            if (realVariants.size() > 1) {
                // Check if all the variants are different versions of the same multi-allelic variant
                Set<String> calls = new HashSet<>();
                for (FillGapsRecord i : realVariants) {
                    String call = i.getCall();
                    if (call == null || call.isEmpty()) {
                        calls.add(null);
                    } else {
                        calls.add(call);
                    }
                }
                // All the variants are from the same call. Select any.
                if (calls.size() == 1 && !calls.contains(null)) {
                    realVariants = Collections.singletonList(realVariants.stream()
                            .min(Comparator.comparing(FillGapsRecord::getCall)).get());
                }
            }
            // If there is only one real variant, use it
            if (realVariants.size() == 1) {
                record = realVariants.get(0);
            } else {
//                String msg = "Found multiple overlaps for variant " + variant + " in file " + fileId;
//                if (!quiet) {
////                    throw new IllegalStateException(msg);
//                    logger.warn(msg);
//                }
                return processMultipleOverlappings(variant, missingSamples, put, fileId);
            }
        } else {
            record = overlappingRecords.get(0);
        }
        Variant archiveVariant = record.convertToVariant(fileId, getSamplePosition(fileId), studyMetadata.getName());

        if (archiveVariant.getType() == VariantType.NO_VARIATION) {
            overlappingStatus = processReferenceOverlap(missingSamples, put, variant, archiveVariant);
        } else {
            overlappingStatus = processVariantOverlap(variant, missingSamples, put, archiveVariant);
        }
        return overlappingStatus;
    }

    protected VariantOverlappingStatus processReferenceOverlap(Set<Integer> missingSamples, Put put,
                                                               Variant variant, Variant archiveVariant) {
        VariantOverlappingStatus overlappingStatus = REFERENCE;

        FileEntry fileEntry = archiveVariant.getStudies().get(0).getFiles().get(0);
        fileEntry.getData().remove(VCFConstants.END_KEY);
        if (fileEntry.getCall() == null) {
            fileEntry.setCall(new OriginalCall(archiveVariant.toString(), 0));
        }
        // SYMBOLIC reference overlap -- <*> , <NON_REF>
        if (VariantType.NO_VARIATION == archiveVariant.getType()
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
            studyEntry.setSampleDataKeys(archiveVariant.getStudies().get(0).getSampleDataKeys());
            studyEntry.setSortedSamplesPosition(new LinkedHashMap<>());
            studyEntry.setSamples(new ArrayList<>());
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
                                                             Variant archiveVariant) {
        VariantOverlappingStatus overlappingStatus = VARIANT;

        Variant mergedVariant = new Variant(
                variant.getChromosome(),
                variant.getStart(),
                variant.getEnd(),
                variant.getReference(),
                variant.getAlternate());
        StudyEntry studyEntry = new StudyEntry();
        mergedVariant.addStudyEntry(studyEntry);
        mergedVariant.setType(variant.getType());
        StudyEntry archiveStudyEntry = archiveVariant.getStudies().get(0);
        if (simplifiedNewMultiAllelicVariants) {
            if (archiveStudyEntry.getSampleDataKeys().contains("GT")) {
                studyEntry.setSampleDataKeys(Collections.singletonList("GT"));

                studyEntry.setSortedSamplesPosition(archiveStudyEntry.getSamplesPosition());
                List<SampleEntry> samplesData = new ArrayList<>(archiveStudyEntry.getSamplesPosition().size());
                studyEntry.setSamples(samplesData);
                studyEntry.getSecondaryAlternates().add(new AlternateCoordinate(
                        variant.getChromosome(),
                        variant.getStart(),
                        variant.getEnd(),
                        variant.getReference(),
                        "<*>",
                        VariantType.SYMBOLIC));

                FileEntry archiveFileEntry = archiveStudyEntry.getFiles().get(0);
                OriginalCall call = archiveFileEntry.getCall();
                if (call == null || StringUtils.isEmpty(call.getVariantId())) {
                    StringBuilder variantId = new StringBuilder(archiveVariant.toString());
                    for (AlternateCoordinate secondaryAlternate : archiveStudyEntry.getSecondaryAlternates()) {
                        variantId.append(",").append(secondaryAlternate.getAlternate());
                    }
                    call = new OriginalCall(variantId.toString(), 0);
                }
                HashMap<String, String> attributes = new HashMap<>();
                archiveFileEntry.getData().computeIfPresent(StudyEntry.FILTER, attributes::put);
                archiveFileEntry.getData().computeIfPresent(StudyEntry.QUAL, attributes::put);
                studyEntry.setFiles(Collections.singletonList(new FileEntry(archiveFileEntry.getFileId(), call, attributes)));

                for (int idx = 0; idx < archiveStudyEntry.getSamplesPosition().size(); idx++) {
                    String gt = archiveStudyEntry.getSampleData(idx).get(0);
                    switch (gt) {
                        case "0/0":
                        case "0|0":
                        case "0":
                            break;
                        case "1/1":
                            gt = "2/2";
                            break;
                        case "0/1":
                            gt = "0/2";
                            break;
                        case "1":
                            gt = "2";
                            break;
                        default:
                            Genotype genotype = new Genotype(gt);
                            int[] allelesIdx = genotype.getAllelesIdx();
                            for (int i = 0; i < allelesIdx.length; i++) {
                                if (allelesIdx[i] > 0) {
                                    allelesIdx[i] = 2;
                                }
                            }
                            gt = genotype.toString();
                            break;
                    }
                    newGenotypes.add(gt);
                    samplesData.add(new SampleEntry(null, null, Collections.singletonList(gt)));
                }
            } else {
                studyEntry.setSampleDataKeys(Collections.emptyList());
            }
        } else {
            studyEntry.setSortedSamplesPosition(new LinkedHashMap<>());
            studyEntry.setSamples(new ArrayList<>());
            studyEntry.setSampleDataKeys(archiveStudyEntry.getSampleDataKeys());
            mergedVariant = variantMerger.merge(mergedVariant, archiveVariant);
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
        List<SampleEntry> samplesData = new ArrayList<>(samplePosition.size());
        for (int i = 0; i < samplePosition.size(); i++) {
            samplesData.add(new SampleEntry(null, null, Collections.singletonList(gt)));
        }

        VariantBuilder builder = Variant.newBuilder(
                variant.getChromosome(),
                variant.getStart(),
                variant.getEnd(),
                variant.getReference(),
                variant.getAlternate())
                .setStudyId(String.valueOf(studyMetadata.getId()))
                .setSampleDataKeys("GT")
                .setFileId(fileId.toString())
                .setSamplesPosition(samplePosition)
                .setSamples(samplesData);

        studyConverter.convert(builder.build(), put, missingSamples, overlappingStatus);
        return overlappingStatus;
    }

    protected VariantOverlappingStatus processMultipleOverlappings(Variant variant, Set<Integer> missingSamples, Put put,
                                                                   Integer fileId) {
        VariantOverlappingStatus overlappingStatus = MULTI;

        String gt = "2/2";
        LinkedHashMap<String, Integer> samplePosition = getSamplePosition(fileId);
        List<SampleEntry> samplesData = new ArrayList<>(samplePosition.size());
        for (int i = 0; i < samplePosition.size(); i++) {
            samplesData.add(new SampleEntry(null, null, Collections.singletonList(gt)));
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
                .setSampleDataKeys("GT")
                .setSamplesPosition(samplePosition)
                .setSamples(samplesData);


//        processVariantOverlap(variant, missingSamples, put, sampleIndexPuts, builder.build());
        studyConverter.convert(builder.build(), put, missingSamples, overlappingStatus);

        return overlappingStatus;
    }

    protected boolean hasMainAlternate(String gt) {
        return StringUtils.contains(gt, '1');
    }

    public boolean getOverlappingVariants(Variant variant, int fileId,
                                          VcfSliceProtos.VcfSlice vcfSlice, ListIterator<VcfSliceProtos.VcfRecord> iterator,
                                          List<Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord>> overlappingRecords) {
        return getOverlappingVariants(variant, fileId, FillGapsRecordVcfSlice.listIterator(vcfSlice, iterator),
                overlappingRecords.stream()
                        .map(p -> new FillGapsRecordVcfSlice(p.getLeft(), p.getRight()))
                        .collect(Collectors.toList()));
    }

    public boolean getOverlappingVariants(Variant variant, int fileId,
                                          ListIterator<FillGapsRecord> iterator,
                                          List<FillGapsRecord> overlappingRecords) {
        Integer resetIteratorIndex = null;
        boolean isAlreadyPresent = false;
        int firstIndex = iterator.nextIndex();
        // Assume sorted VcfRecords
        while (iterator.hasNext()) {
            FillGapsRecord record = iterator.next();
            int start = record.getStart();
            int end = record.getEnd();
            String reference = record.getReference();
            String alternate = record.getAlternate();
            // If the VcfRecord starts after the variant, stop looking for variants
            if (overlapsWith(variant, record.getChromosome(), start, end)) {
                if (resetIteratorIndex == null) {
                    resetIteratorIndex = Math.max(iterator.previousIndex() - 1, firstIndex);
                }

//                if (skipReferenceVariants && hasAllReferenceGenotype(vcfSlice, vcfRecord)) {
//                    // Skip this variant
//                    continue;
//                }

                // If the same variant is present for this file in the VcfSlice, the variant is already loaded
                if (isVariantAlreadyLoaded(variant, record, start, end, reference, alternate)) {
                    // Variant already loaded. Nothing to do!
                    isAlreadyPresent = true;
                    break;
                }

                overlappingRecords.add(record);
            } else if (isRegionAfterVariantStart(start, end, variant)) {
                if (resetIteratorIndex == null) {
                    resetIteratorIndex = Math.max(iterator.previousIndex() - 1, firstIndex);
                }
                // Shouldn't happen that the first VcfRecord from the iterator is beyond the variant to process,
                // and is not the first VcfRecord from the slice.
                // If so, there may be a bug, or the variants or the VcfSlice is not sorted
                if (firstIndex != 0 && firstIndex == iterator.previousIndex()) {
                    // This should never happen
                    throw new IllegalStateException("Variants not in order!"
                            + " First next variant from iterator (index : " + firstIndex + ") "
                            + record.getChromosome() + ':' + start + '-' + end + ':' + reference + ':' + alternate
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
        if (resetIteratorIndex == null && !iterator.hasNext()) {
            // If the iterator reaches the end without finding any point, reset the iterator
            resetIteratorIndex = firstIndex;
        }
        // Send back the iterator
        if (resetIteratorIndex != null) {
//            logger.info("Reset from " + iterator.nextIndex() + " to " + resetIteratorIndex
//                    + ". fileId : " + fileId + " variant " + variant);
            while (iterator.nextIndex() > resetIteratorIndex) {
                iterator.previous();
            }
        }
        return isAlreadyPresent;
    }


    protected interface FillGapsRecord {
        String getAlternate();
        String getReference();
        int getEnd();
        int getStart();
        String getChromosome();
        String getCall();
        boolean isNoVariant();
        boolean hasAnyReferenceGenotype();
        Variant convertToVariant(Integer fileId, LinkedHashMap<String, Integer> samplePosition, String studyName);
    }

    protected static class FillGapsRecordVcfSlice implements FillGapsRecord {
        private final VcfSliceProtos.VcfSlice vcfSlice;
        private final VcfSliceProtos.VcfRecord vcfRecord;

        public FillGapsRecordVcfSlice(VcfSliceProtos.VcfSlice vcfSlice, VcfSliceProtos.VcfRecord vcfRecord) {
            this.vcfSlice = vcfSlice;
            this.vcfRecord = vcfRecord;
        }

        public static ListIterator<FillGapsRecord> listIterator(VcfSliceProtos.VcfSlice vcfSlice) {
            if (vcfSlice == null) {
                return null;
            } else {
                return listIterator(vcfSlice, vcfSlice.getRecordsList().listIterator());
            }
        }

        public static ListIterator<FillGapsRecord> listIterator(VcfSliceProtos.VcfSlice vcfSlice,
                                                                ListIterator<VcfSliceProtos.VcfRecord> it) {
            if (vcfSlice == null) {
                return null;
            } else {
                return new ListIteratorTransformer<>(
                        r -> new FillGapsRecordVcfSlice(vcfSlice, r),
                        r -> ((FillGapsRecordVcfSlice) r).vcfRecord, it);
            }
        }


        @Override
        public String getAlternate() {
            return vcfRecord.getAlternate();
        }
        @Override
        public String getReference() {
            return vcfRecord.getReference();
        }

        @Override
        public int getEnd() {
            return VcfRecordProtoToVariantConverter.getEnd(vcfRecord, vcfSlice.getPosition());
        }

        @Override
        public int getStart() {
            return VcfRecordProtoToVariantConverter.getStart(vcfRecord, vcfSlice.getPosition());
        }

        @Override
        public  String getChromosome() {
            return vcfSlice.getChromosome();
        }

        @Override
        public String getCall() {
            String call = vcfRecord.getCall();
            if (call.isEmpty()) {
                return null;
            } else {
                return call.substring(0, call.lastIndexOf(':'));
            }
        }

        @Override
        public boolean isNoVariant() {
            return vcfRecord.getType() == VariantProto.VariantType.NO_VARIATION;
        }

        @Override
        public Variant convertToVariant(Integer fileId, LinkedHashMap<String, Integer> samplePosition, String studyName) {
            VcfRecordProtoToVariantConverter converter = new VcfRecordProtoToVariantConverter(vcfSlice.getFields(),
                    samplePosition, fileId.toString(), studyName);
            return converter.convert(vcfRecord, vcfSlice.getChromosome(), vcfSlice.getPosition());
        }

        @Override
        public boolean hasAnyReferenceGenotype() {
            return hasAnyReferenceGenotype(vcfSlice, vcfRecord);
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
    }

    protected static class FillGapsRecordVariant implements FillGapsRecord {

        private final Variant variant;

        public FillGapsRecordVariant(Variant variant) {
            this.variant = variant;
        }

        @Override
        public String getAlternate() {
            return variant.getAlternate();
        }

        @Override
        public String getReference() {
            return variant.getReference();
        }

        @Override
        public int getEnd() {
            return variant.getEnd();
        }

        @Override
        public int getStart() {
            return variant.getStart();
        }

        @Override
        public String getChromosome() {
            return variant.getChromosome();
        }

        @Override
        public String getCall() {
            OriginalCall call = variant
                    .getStudies()
                    .get(0)
                    .getFiles()
                    .get(0)
                    .getCall();
            return call != null ? call.getVariantId() : null;
        }

        @Override
        public boolean isNoVariant() {
            return variant.getType() == VariantType.NO_VARIATION;
        }

        @Override
        public boolean hasAnyReferenceGenotype() {
//            return variant.getStudies().get(0).getSamples()
//                    .stream()
//                    .map(s -> s.getData().get(0))
//                    .anyMatch(FillGapsTask::isHomRefDiploid);
            // FIXME: Is this correct?
            return variant.getStudies().get(0).getSamples()
                    .stream()
                    .map(s -> s.getData().get(0))
                    .allMatch(FillGapsTask::isHomRefDiploid);
        }

        @Override
        public Variant convertToVariant(Integer fileId, LinkedHashMap<String, Integer> samplePosition, String studyName) {
            return variant;
        }

        public static ListIterator<FillGapsRecord> listIterator(ListIterator<Variant> it) {
            return new ListIteratorTransformer<>(FillGapsRecordVariant::new, r -> ((FillGapsRecordVariant) r).variant, it);
        }
    }

    private static final class ListIteratorTransformer<I, E> implements ListIterator<E> {

        private final Function<I, E> in;
        private final Function<E, I> out;
        private final ListIterator<I> it;

        private ListIteratorTransformer(Function<I, E> in, Function<E, I> out, ListIterator<I> it) {
            this.in = in;
            this.out = out;
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public E next() {
            return in.apply(it.next());
        }

        @Override
        public boolean hasPrevious() {
            return it.hasPrevious();
        }

        @Override
        public E previous() {
            return in.apply(it.previous());
        }

        @Override
        public int nextIndex() {
            return it.nextIndex();
        }

        @Override
        public int previousIndex() {
            return it.previousIndex();
        }

        @Override
        public void remove() {
            it.remove();
        }

        @Override
        public void set(E e) {
            it.set(out.apply(e));
        }

        @Override
        public void add(E e) {
            it.add(out.apply(e));
        }
    }

    /**
     * Check if this VcfRecord is already loaded in the variant that is being processed.
     *
     * If so, the variant does not have a gap for this file. Nothing to do!
     */
    private boolean isVariantAlreadyLoaded(Variant variant, FillGapsRecord record,
                                           int start, int end, String reference, String alternate) {
        // The variant is not loaded if is a NO_VARIATION (fast check first)
        if (record.isNoVariant()) {
            return false;
        }
        // Check if the variant is the same
        if (!variant.sameGenomicVariant(new Variant(record.getChromosome(), start, end, reference, alternate))) {
            return false;
        }
        // If any of the genotypes is HOM_REF, the variant won't be completely loaded, so there may be a gap.
        return !record.hasAnyReferenceGenotype();
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

    /**
     * Checks if the given region is entirely or partially (overlapping) after the variant start.
     *
     * @param start Start of a region
     * @param end   End of a region (might be before the start if the region represented an insertion)
     * @param variant Variant to check
     * @return  if the region is after the variant
     */
    public static boolean isRegionAfterVariantStart(int start, int end, Variant variant) {
        // Get region max position. In the region represents an insertion, the start might be after the end
        int regionMaxPos = Math.max(start, end);
        int variantMinPos = Math.min(variant.getStart(), variant.getEnd());
        return regionMaxPos > variantMinPos;
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

    protected void updateLoadedGenotypes() throws StorageEngineException {
        Set<String> loadedGenotypes = new LinkedHashSet<>(studyMetadata.getAttributes()
                .getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key()));
        if (!loadedGenotypes.containsAll(newGenotypes)) {
            loadedGenotypes.addAll(newGenotypes);

            metadataManager.updateStudyMetadata(studyMetadata, sm -> {
                sm.getAttributes().put(VariantStorageOptions.LOADED_GENOTYPES.key(), loadedGenotypes);
            });
        }
    }
}
