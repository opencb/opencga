package org.opencb.opencga.storage.alignment;

import com.google.protobuf.ByteString;
import org.opencb.commons.bioformats.alignment.Alignment;

import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jmmut
 * Date: 3/7/14
 * Time: 11:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class ProtoToAlignment {

    public static Alignment toAlignment(AlignmentProto.AlignmentRecord alignmentRecord, String chromosome, int start){

        return new Alignment(
                alignmentRecord.getName(),
                chromosome,
                start,
                start + alignmentRecord.getLen(),
                start,  // FIXME jj <unclipped start> does not always equal to <start>
                start + alignmentRecord.getLen(),   // FIXME jj same here
                alignmentRecord.getLen(),
                alignmentRecord.getMapq(),
                alignmentRecord.getQualities(),
                alignmentRecord.getRnext(),
                alignmentRecord.getRelativePnext() + start,
                alignmentRecord.getInferredInsertSize(),
                alignmentRecord.getFlags(),
                toAlignmentDifference(alignmentRecord.getDiffsList()),
                null    // FIXME get real attributes
                );
    }

    public static AlignmentProto.AlignmentRecord toProto(Alignment alignment){

        AlignmentProto.AlignmentRecord.Builder alignmentRecordBuilder = AlignmentProto.AlignmentRecord.newBuilder()
                .setName(alignment.getName())
                .setFlags(alignment.getFlags())
                .setPos((int) alignment.getStart())   //TODO jj: Real Incremental Pos
                .setMapq(alignment.getMappingQuality())
                .setRnext("rnext")
                .setRelativePnext(alignment.getMateAlignmentStart())
                .setLen(alignment.getLength());

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

            alignmentRecordBuilder.addDiffs(AlignmentProto.Difference.newBuilder()
                    .setOperator(operator)
                    .setPos(alignmentDifference.getPos())
                    .setLength(alignmentDifference.getLength())
                    .setSequence(ByteString.copyFromUtf8(alignmentDifference.getSeq())) // TODO check this works properly
                    .build()
            );
        }

        return alignmentRecordBuilder.build();
    }

    public static List<Alignment.AlignmentDifference> toAlignmentDifference(List<AlignmentProto.Difference> differenceList) {
        List<Alignment.AlignmentDifference> alignmentDifferenceList = new LinkedList<>();
        for (AlignmentProto.Difference difference: differenceList) {
            char operator = AlignmentProto.Difference.DifferenceOperator.MISMATCH_VALUE;
            switch(difference.getOperator().getNumber()) {
                case AlignmentProto.Difference.DifferenceOperator.DELETION_VALUE:
                    operator = Alignment.AlignmentDifference.DELETION;
                    break;
                case AlignmentProto.Difference.DifferenceOperator.HARD_CLIPPING_VALUE:
                    operator = Alignment.AlignmentDifference.HARD_CLIPPING;
                    break;
                case AlignmentProto.Difference.DifferenceOperator.INSERTION_VALUE:
                    operator = Alignment.AlignmentDifference.INSERTION;
                    break;
                case AlignmentProto.Difference.DifferenceOperator.MISMATCH_VALUE:
                    operator = Alignment.AlignmentDifference.MISMATCH;
                    break;
                case AlignmentProto.Difference.DifferenceOperator.PADDING_VALUE:
                    operator = Alignment.AlignmentDifference.PADDING;
                    break;
                case AlignmentProto.Difference.DifferenceOperator.SKIPPED_REGION_VALUE:
                    operator = Alignment.AlignmentDifference.SKIPPED_REGION;
                    break;
                case AlignmentProto.Difference.DifferenceOperator.SOFT_CLIPPING_VALUE:
                    operator = Alignment.AlignmentDifference.SOFT_CLIPPING;
                    break;
            }

            alignmentDifferenceList.add(new Alignment.AlignmentDifference(difference.getPos(), operator, String.valueOf(difference.getSequence())));
        }

        return alignmentDifferenceList;
    }
}
