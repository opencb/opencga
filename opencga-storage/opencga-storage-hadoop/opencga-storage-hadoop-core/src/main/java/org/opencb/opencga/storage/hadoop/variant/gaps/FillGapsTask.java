package org.opencb.opencga.storage.hadoop.variant.gaps;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
    private final boolean fillOnlyMissingGenotypes;

    private Logger logger = LoggerFactory.getLogger(FillGapsTask.class);

    public FillGapsTask(StudyConfiguration studyConfiguration, GenomeHelper helper, boolean fillOnlyMissingGenotypes) {
        this.studyConfiguration = studyConfiguration;
        this.fillOnlyMissingGenotypes = fillOnlyMissingGenotypes;

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
        // TODO: What if multiple overlapps?
        boolean found = false;
        for (VcfSliceProtos.VcfRecord vcfRecord : vcfSlice.getRecordsList()) {
            int start = VcfRecordProtoToVariantConverter.getStart(vcfRecord, position);
            int end = VcfRecordProtoToVariantConverter.getEnd(vcfRecord, position);
            String reference = vcfRecord.getReference();
            String alternate = vcfRecord.getAlternate();
            if (overlapsWith(variant, chromosome, start, end)) {
                if (VcfRecordProtoToVariantConverter.getVariantType(vcfRecord.getType()).equals(VariantType.NO_VARIATION)) {
                    if (!fillOnlyMissingGenotypes || hasMissingGenotype(vcfSlice, vcfRecord)) {
                        Variant archiveVariant = convertToVariant(vcfSlice, vcfRecord, fileId);
                        FileEntry fileEntry = archiveVariant.getStudies().get(0).getFiles().get(0);
                        fileEntry.getAttributes().remove(VCFConstants.END_KEY);
                        if (StringUtils.isEmpty(fileEntry.getCall())) {
                            fileEntry.setCall(archiveVariant.getStart() + ":" + archiveVariant.getReference() + ":.:0");
                        }
                        studyConverter.convert(archiveVariant, put, missingSamples);
                        found = true;
                    }
                } else if (!variant.sameGenomicVariant(new Variant(chromosome, start, end, reference, alternate))) {
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
                    found = true;
                }
            }
        }
        if (!found) {
            logger.debug("Not overlap for fileId " + fileId + " in variant " + variant);
        }
    }



    public boolean hasMissingGenotype(VcfSliceProtos.VcfSlice vcfSlice, VcfSliceProtos.VcfRecord vcfRecord) {
        boolean hasMissingGenotype = false;
        for (VcfSliceProtos.VcfSample vcfSample : vcfRecord.getSamplesList()) {
            if (vcfSlice.getFields().getGts(vcfSample.getGtIndex()).contains(".")) {
                hasMissingGenotype = true;
                break;
            }
        }
        return hasMissingGenotype;
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
