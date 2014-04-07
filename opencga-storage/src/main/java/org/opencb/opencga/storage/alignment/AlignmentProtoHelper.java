package org.opencb.opencga.storage.alignment;

import com.google.protobuf.ByteString;
import org.opencb.commons.bioformats.alignment.Alignment;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: jmmut
 * Date: 3/7/14
 * Time: 11:29 AM
 *
 * TODO jj: check rnext, pnext, mateAlignmentStart,
 * TODO jj: check another sam with CIGAR: hard clipping, padding, and skipped region
 */
public class AlignmentProtoHelper {


    public static AlignmentProto.AlignmentBucket toAlignmentBucketProto(List<Alignment> alignments, AlignmentRegionSummary summary, long bucketStart){

        AlignmentProto.AlignmentBucket.Builder alignmentBucketBuilder = AlignmentProto.AlignmentBucket.newBuilder();
        long prevStart = bucketStart;

        for(Alignment alignment : alignments){
            alignmentBucketBuilder.addAlignmentRecords(AlignmentProtoHelper.toProto(alignment,prevStart, summary));
            prevStart = alignment.getStart();
        }

        return alignmentBucketBuilder.build();
    }

    private static AlignmentProto.AlignmentRecord toProto(Alignment alignment, long prevStart, AlignmentRegionSummary summary){
        AlignmentProto.AlignmentRecord.Builder alignmentRecordBuilder = AlignmentProto.AlignmentRecord.newBuilder()
                .setName(alignment.getName())
                .setPos((int) (alignment.getStart() - prevStart))
                .setMapq(alignment.getMappingQuality())
                .setRelativePnext(alignment.getMateAlignmentStart())
                .setQualities(alignment.getQualities())
                .setInferredInsertSize(alignment.getInferredInsertSize());

        if(alignment.getFlags() != summary.getDefaultFlag()){
            alignmentRecordBuilder.setFlags(alignment.getFlags());
        }
        if(alignment.getLength() != summary.getDefaultLen()){
            alignmentRecordBuilder.setLen(alignment.getLength());
        }
        if(!alignment.getMateReferenceName().equals(summary.getDefaultRNext())){
            alignmentRecordBuilder.setRnext(alignment.getMateReferenceName());
        }

        int prevDifferencePos = 0;
        for(Alignment.AlignmentDifference alignmentDifference : alignment.getDifferences()){
            AlignmentProto.Difference.DifferenceOperator operator = AlignmentProto.Difference.DifferenceOperator.MISMATCH;
            switch(alignmentDifference.getOp()){
                case Alignment.AlignmentDifference.DELETION:
                    operator = AlignmentProto.Difference.DifferenceOperator.DELETION;
                    break;
                case Alignment.AlignmentDifference.HARD_CLIPPING:
                    operator = AlignmentProto.Difference.DifferenceOperator.HARD_CLIPPING;
                    break;
                case Alignment.AlignmentDifference.INSERTION:
                    operator = AlignmentProto.Difference.DifferenceOperator.INSERTION;
                    break;
                case Alignment.AlignmentDifference.MISMATCH:
                    operator = AlignmentProto.Difference.DifferenceOperator.MISMATCH;
                    break;
                case Alignment.AlignmentDifference.PADDING:
                    operator = AlignmentProto.Difference.DifferenceOperator.PADDING;
                    break;
                case Alignment.AlignmentDifference.SKIPPED_REGION:
                    operator = AlignmentProto.Difference.DifferenceOperator.SKIPPED_REGION;
                    break;
                case Alignment.AlignmentDifference.SOFT_CLIPPING:
                    operator = AlignmentProto.Difference.DifferenceOperator.SOFT_CLIPPING;
                    break;
            }

            AlignmentProto.Difference.Builder differenceBuilder = AlignmentProto.Difference.newBuilder()
                    .setOperator(operator);     //TODO jj: Default operator

            if(alignmentDifference.getPos() != prevDifferencePos){
                differenceBuilder.setPos(alignmentDifference.getPos());
            }

            if (alignmentDifference.isSequenceStored()) {
                differenceBuilder.setSequence(ByteString.copyFromUtf8(alignmentDifference.getSeq()));
            } else {
                if(alignmentDifference.getLength() != 1){    //TODO jj: Default length?
                    differenceBuilder.setLength(alignmentDifference.getLength());
                }
            }
            alignmentRecordBuilder.addDiffs(differenceBuilder.build());
        }
        alignmentRecordBuilder.addAllTags(summary.getIndexTagList(alignment.getAttributes()));

        return alignmentRecordBuilder.build();
    }



    public List<Alignment> fromAlignmentBucketProto(AlignmentProto.AlignmentBucket){


        throw new UnsupportedOperationException();
    }



    /**
     *
     * @param alignmentRecord
     * @param chromosome
     * @param chunkStart start of the alignmentRegion.
     * @return
     */
    public static Alignment toAlignment(AlignmentProto.AlignmentRecord alignmentRecord, String chromosome, long chunkStart){

        Map<String, String> attributes = new HashMap<>();
        for (AlignmentProto.MyMap.Pair pair: alignmentRecord.getTags().getPairList()) {
            attributes.put(pair.getKey(), pair.getValue());
        }
        LinkedList<Alignment.AlignmentDifference> alignmentDifferences = new LinkedList<>();
        long offset = toAlignmentDifference(alignmentRecord.getDiffsList(), alignmentDifferences);
        long unclippedStartOffset = 0;
        long end = (alignmentRecord.getFlags() & 0x4) > 0 ? 0: alignmentRecord.getPos() + chunkStart + alignmentRecord.getLen() -1 + offset;
        Alignment.AlignmentDifference alignmentDifference = alignmentDifferences.size() > 0? alignmentDifferences.get(0): null;
        if (alignmentDifference != null) {
            if (alignmentDifference.getOp() == Alignment.AlignmentDifference.SOFT_CLIPPING
                    && alignmentDifference.getPos() == 0) { // soft clipping at the  start
                unclippedStartOffset = alignmentDifference.getLength();
            }
        }
        int unclippedEndOffset = 0;
        alignmentDifference = alignmentDifferences.size() > 0? alignmentDifferences.getLast(): null;
        if (alignmentDifference != null) {
            if (alignmentDifference.getOp() == Alignment.AlignmentDifference.SOFT_CLIPPING
                    && alignmentDifference.getPos() != 0 ){ // soft cliping at the end
                unclippedEndOffset = alignmentDifference.getLength();
            }
        }


        return new Alignment(
                alignmentRecord.getName(),
                chromosome,
                alignmentRecord.getPos() + chunkStart,
                end,
                alignmentRecord.getPos() + chunkStart - unclippedStartOffset,  // FIXME jj <unclipped start> does not always equal to <start>
                end + unclippedEndOffset,   // FIXME jj same here
                alignmentRecord.getLen(),
                alignmentRecord.getMapq(),
                alignmentRecord.getQualities(),
                alignmentRecord.getRnext(),
                alignmentRecord.getRelativePnext(), // TODO jj check
                alignmentRecord.getInferredInsertSize(),
                alignmentRecord.getFlags(),
                alignmentDifferences,
                attributes
                //TODO: ).setReadSequence();
                );
    }





    public static  int toAlignmentDifference(List<AlignmentProto.Difference> differenceList, List<Alignment.AlignmentDifference> alignmentDifferenceList) {
        int offset = 0;
        for (AlignmentProto.Difference difference: differenceList) {
            char operator = AlignmentProto.Difference.DifferenceOperator.MISMATCH_VALUE;
            switch(difference.getOperator().getNumber()) {
                case AlignmentProto.Difference.DifferenceOperator.DELETION_VALUE:
                    operator = Alignment.AlignmentDifference.DELETION;
                    offset += difference.getLength();
                    break;
                case AlignmentProto.Difference.DifferenceOperator.HARD_CLIPPING_VALUE:
                    operator = Alignment.AlignmentDifference.HARD_CLIPPING; // FIXME offset
                    break;
                case AlignmentProto.Difference.DifferenceOperator.INSERTION_VALUE:
                    operator = Alignment.AlignmentDifference.INSERTION;
                    offset -= difference.getLength();
                    break;
                case AlignmentProto.Difference.DifferenceOperator.MISMATCH_VALUE:
                    operator = Alignment.AlignmentDifference.MISMATCH;
                    break;
                case AlignmentProto.Difference.DifferenceOperator.PADDING_VALUE:
                    operator = Alignment.AlignmentDifference.PADDING; // FIXME offset
                    break;
                case AlignmentProto.Difference.DifferenceOperator.SKIPPED_REGION_VALUE:
                    operator = Alignment.AlignmentDifference.SKIPPED_REGION; // FIXME offset
                    break;
                case AlignmentProto.Difference.DifferenceOperator.SOFT_CLIPPING_VALUE:
                    operator = Alignment.AlignmentDifference.SOFT_CLIPPING;
                    offset -= difference.getLength();
                    break;
            }


            String readSequence = difference.hasSequence()? new String(difference.getSequence().toByteArray()): null;
            if (readSequence == null) {
                alignmentDifferenceList.add(new Alignment.AlignmentDifference(difference.getPos(), operator, difference.getLength()));
            } else {
                alignmentDifferenceList.add(new Alignment.AlignmentDifference(difference.getPos(), operator, readSequence));
            }
        }

        return offset;
    }

    public static long getPositionFromRowkey(String rowKey){
        return Long.valueOf(rowKey.split("_")[1]) << 8;
    }
    public static String getChromosomeFromRowkey(String rowKey){
        return rowKey.split("_")[0];
    }
}
