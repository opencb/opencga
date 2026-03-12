package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.gaps.AbstractFillGapsTask;
import org.opencb.opencga.storage.core.variant.gaps.VariantOverlappingStatus;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryMultiFileToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;

import java.util.*;
import java.util.stream.Collectors;

/**
 * HBase implementation of {@link AbstractFillGapsTask}.
 * Writes gap-filled data as HBase {@link Put} mutations via {@link StudyEntryToHBaseConverter}.
 *
 * Created on 15/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseFillGapsTask extends AbstractFillGapsTask {

    private final StudyEntryToHBaseConverter studyConverter;
    private Put put;

    public HBaseFillGapsTask(VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata, boolean skipReferenceVariants,
                             boolean simplifiedNewMultiAllelicVariants, String gapsGenotype) {
        super(metadataManager, studyMetadata, skipReferenceVariants, simplifiedNewMultiAllelicVariants, gapsGenotype);
        studyConverter = new StudyEntryMultiFileToHBaseConverter(GenomeHelper.COLUMN_FAMILY_BYTES, studyMetadata.getId(), metadataManager,
                true,
                null, // Do not update release
                true, // Do not skip any genotype
                false);
    }

    @Override
    protected void write(Variant variant, Set<Integer> missingSamples, VariantOverlappingStatus status) {
        studyConverter.convert(variant, put, missingSamples, status);
    }

    // ─── fillGaps overloads that accept HBase Put ────────────────────────────

    public VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put,
                                             Integer fileId,
                                             VcfSliceProtos.VcfSlice nonRefVcfSlice, VcfSliceProtos.VcfSlice refVcfSlice) {
        return fillGaps(variant, missingSamples, put, fileId,
                FillGapsRecordVcfSlice.listIterator(nonRefVcfSlice),
                FillGapsRecordVcfSlice.listIterator(refVcfSlice));
    }

    public VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put,
                                             Integer fileId,
                                             VcfSliceProtos.VcfSlice nonRefVcfSlice,
                                             ListIterator<VcfSliceProtos.VcfRecord> nonRefIterator,
                                             VcfSliceProtos.VcfSlice refVcfSlice,
                                             ListIterator<VcfSliceProtos.VcfRecord> refIterator) {
        return fillGaps(variant, missingSamples, put, fileId,
                FillGapsRecordVcfSlice.listIterator(nonRefVcfSlice, nonRefIterator),
                FillGapsRecordVcfSlice.listIterator(refVcfSlice, refIterator));
    }

    public VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put,
                                             Integer fileId,
                                             ListIterator<Variant> nonRefVariants) {
        this.put = put;
        return super.fillGaps(variant, missingSamples, fileId, nonRefVariants);
    }

    private VariantOverlappingStatus fillGaps(Variant variant, Set<Integer> missingSamples, Put put,
                                              Integer fileId,
                                              ListIterator<FillGapsRecord> nonRefIterator,
                                              ListIterator<FillGapsRecord> refIterator) {
        this.put = put;
        return super.fillGaps(variant, missingSamples, fileId, nonRefIterator, refIterator);
    }

    public boolean getOverlappingVariants(Variant variant, int fileId,
                                          VcfSliceProtos.VcfSlice vcfSlice, ListIterator<VcfSliceProtos.VcfRecord> iterator,
                                          List<Pair<VcfSliceProtos.VcfSlice, VcfSliceProtos.VcfRecord>> overlappingRecords) {
        return getOverlappingVariants(variant, fileId, FillGapsRecordVcfSlice.listIterator(vcfSlice, iterator),
                overlappingRecords.stream()
                        .map(p -> (FillGapsRecord) new FillGapsRecordVcfSlice(p.getLeft(), p.getRight()))
                        .collect(Collectors.toList()));
    }

    // ─── VcfSlice-based FillGapsRecord (HBase-specific, depends on protobuf) ─

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
        public String getChromosome() {
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

    protected static boolean hasAllReferenceGenotype(VcfSliceProtos.VcfSlice vcfSlice, VcfSliceProtos.VcfRecord vcfRecord) {
        for (VcfSliceProtos.VcfSample vcfSample : vcfRecord.getSamplesList()) {
            String gt = vcfSlice.getFields().getGts(vcfSample.getGtIndex());
            if (!isHomRefDiploid(gt)) {
                return false;
            }
        }
        return true;
    }
}
