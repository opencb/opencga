package org.opencb.opencga.storage.alignment.proto;

import com.google.protobuf.ByteString;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.opencga.storage.alignment.AlignmentRegionSummary;

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


    public static AlignmentProto.AlignmentBucket toAlignmentBucketProto(List<Alignment> alignments, AlignmentRegionSummary summary, long bucketStart, int overlapped){
        if(alignments == null || alignments.isEmpty()){
            if(overlapped != 0){
                if(alignments == null){
                    alignments = new LinkedList<>();
                }
            } else {
                return null;
            }
        }
        AlignmentProto.AlignmentBucket.Builder alignmentBucketBuilder = AlignmentProto.AlignmentBucket.newBuilder();
        alignmentBucketBuilder.setOverlapped(overlapped);
        alignmentBucketBuilder.setSummaryIndex(summary.getIndex());
        long prevStart = bucketStart;

        for(Alignment alignment : alignments){
            alignmentBucketBuilder.addAlignmentRecords(AlignmentProtoHelper.toProto(alignment,prevStart, summary));
            prevStart = alignment.getStart();
        }

        return alignmentBucketBuilder.build();
    }

    public static AlignmentProto.AlignmentRecord toProto(Alignment alignment, long prevStart, AlignmentRegionSummary summary){
        AlignmentProto.AlignmentRecord.Builder alignmentRecordBuilder = AlignmentProto.AlignmentRecord.newBuilder()
                .setName(alignment.getName())
                .setPos((int) (alignment.getStart() - prevStart))
                .setMapq(alignment.getMappingQuality())
                .setRelativePnext((int)(alignment.getMateAlignmentStart() - alignment.getStart()))
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



    public static List<Alignment> fromAlignmentBucketProto(AlignmentProto.AlignmentBucket alignmentBucket, AlignmentRegionSummary summary, String chromosome, long bucketStart){

        if(alignmentBucket.getSummaryIndex() != summary.getIndex()){
            System.out.println("[ERROR] Summary doesn't match!"); //TODO jj: Throw exception?
        }

        List<Alignment> alignments = new LinkedList<>();
        long prevStart = bucketStart;
        Alignment alignment;

        for(AlignmentProto.AlignmentRecord  alignmentRecord : alignmentBucket.getAlignmentRecordsList()){
            alignments.add(alignment = toAlignment(alignmentRecord, summary, chromosome, prevStart));
            prevStart = alignment.getStart();
        }

        return alignments;
    }



    public static Alignment toAlignment(AlignmentProto.AlignmentRecord alignmentProto, AlignmentRegionSummary summary, String chromosome, long prevStart){


        List<Alignment.AlignmentDifference> alignmentDifferences = new LinkedList<>();
        int offset = toAlignmentDifference(alignmentProto.getDiffsList(), alignmentDifferences);

        int length = alignmentProto.hasLen() ? alignmentProto.getLen() : summary.getDefaultLen();
        long start = alignmentProto.getPos() + prevStart;
        long end   = start + offset + length - 1;
        long unclippedStart = start;
        long unclippedEnd = end;

        if(!alignmentDifferences.isEmpty() && alignmentDifferences.get(0).getOp() == Alignment.AlignmentDifference.SOFT_CLIPPING){
            unclippedStart -= alignmentDifferences.get(0).getLength();
        }
        if(!alignmentDifferences.isEmpty() && alignmentDifferences.get(alignmentDifferences.size()-1).getOp() == Alignment.AlignmentDifference.SOFT_CLIPPING){
            unclippedEnd += alignmentDifferences.get(alignmentDifferences.size()-1).getLength();
        }

        Alignment alignment = new Alignment();


        alignment.setName(alignmentProto.getName());
        alignment.setChromosome(chromosome);
        alignment.setStart(start);
        alignment.setEnd(end);
        alignment.setUnclippedStart(unclippedStart);
        alignment.setUnclippedEnd(unclippedEnd);
        alignment.setLength(length);                                                                                         //Optiona. Get from Summary
        alignment.setMappingQuality(alignmentProto.getMapq());
        alignment.setQualities(alignmentProto.getQualities());
        alignment.setMateAlignmentStart((int) (alignmentProto.getRelativePnext() + start));
        alignment.setMateReferenceName(alignmentProto.hasRnext() ? alignmentProto.getRnext() : summary.getDefaultRNext());    //Optiona. Get from Summary
        alignment.setInferredInsertSize(alignmentProto.getInferredInsertSize());
        alignment.setFlags(alignmentProto.hasFlags() ? alignmentProto.getFlags() : summary.getDefaultFlag());                 //Optiona. Get from Summary
        alignment.setDifferences(alignmentDifferences);
        alignment.setAttributes(summary.getTagsFromList(alignmentProto.getTagsList()));



        return alignment;
    }

    /* Compression Core. UNIMPLEMENTED!
     *
        000  A
        001  C
        010  G
        011  T
        100  N
        101
        110
        111  END

     */
    private static String uncompressSeq(ByteString seq){
        //String readSequence = difference.hasSequence()? new String(difference.getSequence().toByteArray()): null;

        throw new UnsupportedOperationException();
    }


    private static ByteString compressSeq(String seq){
        throw new UnsupportedOperationException();
    }


    public static  int toAlignmentDifference(List<AlignmentProto.Difference> differenceList, List<Alignment.AlignmentDifference> alignmentDifferenceList) {
        int offset = 0;
        int prevPos = 0;
        for (AlignmentProto.Difference difference: differenceList) {

            int pos = difference.hasPos() ? difference.getPos() : prevPos;                          //If miss, prev position.
            prevPos = pos;  //update prev position.
            String seq = difference.hasSequence()? new String(difference.getSequence().toByteArray()) : null;  //If miss, null
            int len = difference.hasLength()?difference.getLength() : seq!=null ? seq.length() : 1; //If miss, seq.length. If miss too, 1.
            char operator = AlignmentProto.Difference.DifferenceOperator.MISMATCH_VALUE;

            switch(difference.getOperator().getNumber()) {
                case AlignmentProto.Difference.DifferenceOperator.DELETION_VALUE:
                    operator = Alignment.AlignmentDifference.DELETION;
                    offset += len;
                    break;
                case AlignmentProto.Difference.DifferenceOperator.HARD_CLIPPING_VALUE:
                    operator = Alignment.AlignmentDifference.HARD_CLIPPING; // FIXME offset
                    break;
                case AlignmentProto.Difference.DifferenceOperator.INSERTION_VALUE:
                    operator = Alignment.AlignmentDifference.INSERTION;
                    offset -= len;
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
                    offset -= len;
                    break;
            }

            alignmentDifferenceList.add(new Alignment.AlignmentDifference(pos, operator, seq, len));

        }

        return offset;
    }

    public static long getPositionFromRowkey(String rowKey, int bucketSize){
        return Long.valueOf(rowKey.split("_")[1]) * bucketSize;
    }
    public static String getChromosomeFromRowkey(String rowKey){
        return rowKey.split("_")[0];
    }

    public static String getBucketRowkey(String chromosome, long start, int bucketSize){
        return getBucketRowkey(chromosome, start/bucketSize);
    }
    public static String getBucketRowkey(String chromosome, long bucketIndex){
        return chromosome + "_" + String.format("%07d", bucketIndex);
    }
    public static String getSummaryRowkey(String chromosome, int index){
        return  "S_" + chromosome + "_" + String.format("%05d", index);
    }
}
