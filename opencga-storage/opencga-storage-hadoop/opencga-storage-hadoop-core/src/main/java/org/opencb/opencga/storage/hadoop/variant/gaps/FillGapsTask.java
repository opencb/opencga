package org.opencb.opencga.storage.hadoop.variant.gaps;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

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

    private Logger logger = LoggerFactory.getLogger(FillGapsTask.class);

    public FillGapsTask(StudyConfiguration studyConfiguration, GenomeHelper helper, boolean skipReferenceVariants) {
        this.studyConfiguration = studyConfiguration;
        this.skipReferenceVariants = skipReferenceVariants;

        studyConverter = new StudyEntryToHBaseConverter(helper.getColumnFamily(), studyConfiguration, true, Collections.singleton("?/?"));
        variantMerger = new VariantMerger(false).configure(studyConfiguration.getVariantHeader());

    }

    public void fillGaps(Variant variant, Set<Integer> missingSamples, Put put, Integer fileId, VcfSliceProtos.VcfSlice vcfSlice) {
        String chromosome = vcfSlice.getChromosome();
        int position = vcfSlice.getPosition();

        // Three scenarios:
        //  Overlap with NO_VARIATION,
        //  Overlap with another variant
        //  No overlap

        List<VcfSliceProtos.VcfRecord> overlappingRecords = new ArrayList<>(1);
        for (VcfSliceProtos.VcfRecord vcfRecord : vcfSlice.getRecordsList()) {
            int start = VcfRecordProtoToVariantConverter.getStart(vcfRecord, position);
            int end = VcfRecordProtoToVariantConverter.getEnd(vcfRecord, position);
            String reference = vcfRecord.getReference();
            String alternate = vcfRecord.getAlternate();
            if (overlapsWith(variant, chromosome, start, end)) {
                if (skipReferenceVariants && hasAllReferenceGenotype(vcfSlice, vcfRecord)) {
                    // Skip this variant
                    continue;
                }

                // If the same variant is present for this file in the VcfSlice, the variant is already loaded
                if (isVariantAlreadyLoaded(variant, vcfSlice, vcfRecord, chromosome, start, end, reference, alternate)) {
                    // Variant already loaded. Nothing to do!
                    return;
                }

                overlappingRecords.add(vcfRecord);
            }
        }

        VcfSliceProtos.VcfRecord vcfRecord;
        if (overlappingRecords.isEmpty()) {
            // TODO: There was a gap in the gVCF?
            // May happen that the variant to fill is an insertion, and there is no overlapping
            // Should write "./." ? "0/0" for insertions?
            logger.debug("Not overlap for fileId " + fileId + " in variant " + variant);
            // Nothing to do!
            return;
        } else if (overlappingRecords.size() > 1) {
            // TODO: What if multiple overlaps?

            // Discard ref_blocks
            List<VcfSliceProtos.VcfRecord> realVariants = overlappingRecords
                    .stream()
                    .filter(record -> record.getType() != VariantProto.VariantType.NO_VARIATION)
                    .collect(Collectors.toList());
            // If there is only one real variant, use it
            if (realVariants.size() == 1) {
                vcfRecord = realVariants.get(0);
            } else {
                throw new IllegalStateException("Found multiple overlaps for variant " + variant + " in file " + fileId);
            }
        } else {
            vcfRecord = overlappingRecords.get(0);
        }
        if (VcfRecordProtoToVariantConverter.getVariantType(vcfRecord.getType()).equals(VariantType.NO_VARIATION)) {
            Variant archiveVariant = convertToVariant(vcfSlice, vcfRecord, fileId);
            FileEntry fileEntry = archiveVariant.getStudies().get(0).getFiles().get(0);
            fileEntry.getAttributes().remove(VCFConstants.END_KEY);
            if (StringUtils.isEmpty(fileEntry.getCall())) {
                fileEntry.setCall(archiveVariant.getStart() + ":" + archiveVariant.getReference() + ":.:0");
            }
            studyConverter.convert(archiveVariant, put, missingSamples);
        } else {
            Variant archiveVariant = convertToVariant(vcfSlice, vcfRecord, fileId);
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
            studyConverter.convert(mergedVariant, put, missingSamples);
        }
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
            if (gt.equals("0/0") || gt.equals("0|0")) {
                return true;
            }
        }
        return false;
    }

    protected static boolean hasAllReferenceGenotype(VcfSliceProtos.VcfSlice vcfSlice, VcfSliceProtos.VcfRecord vcfRecord) {
        for (VcfSliceProtos.VcfSample vcfSample : vcfRecord.getSamplesList()) {
            String gt = vcfSlice.getFields().getGts(vcfSample.getGtIndex());
            if (!gt.equals("0/0") && !gt.equals("0|0")) {
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
