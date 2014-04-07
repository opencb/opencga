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

    public static class Summary{

        private int index;

        private int defaultFlag;
        private int defaultLen;
        private String defaultRNext;
        private int defaultOverlapped;
        private Tags tags;

        //Histogram for default values.
        private Map<Integer, Integer> flagsMap = new HashMap<>();
        private Map<Integer, Integer> lenMap = new HashMap<>();
        private Map<String, Integer> rnextMap = new HashMap<>();

        public static class Tags {
            //fromTags
            private Map<String, Integer> keysMap = new HashMap<>();
            private Map<Map.Entry<Integer, Object>, Integer> tagsMap = new HashMap<>();

            //toTags
            private List<String> keysList = null;

            public Map<String, Object> toTags(int[] tags){
                throw new UnsupportedOperationException();
            }

            public String getKeys(){
                String keys = "";
                keysList = new ArrayList<String>(keysMap.size());

                for(Map.Entry<String, Integer> entry : keysMap.entrySet()){
                    keysList.set(entry.getValue(), entry.getKey());
                }
                for(String s : keysList){
                    keys += s;
                }
                return keys;
            }

            public ArrayList<AlignmentProto.Summary.Pair> getAllValues(){
                ArrayList<AlignmentProto.Summary.Pair> listP = new ArrayList<>(tagsMap.size());

                for(Map.Entry<Map.Entry<Integer, Object>, Integer> entry : tagsMap.entrySet()){
                    if(listP.set(entry.getValue(),
                            AlignmentProto.Summary.Pair.newBuilder()
                                .setKey(entry.getKey().getKey())
                                .setAvalue(entry.getKey().getValue().toString())
                                .build()
                    ) != null){
                        System.out.println("[ERROR] Duplicated tag index.");
                    }
                }
                return listP;
            }

            /**
             * From a tag map, update the tag list, and return the indices list
             *
             * @param tags Map of attributes
             * @return List of indices
             */
            public int[] fromTags(Map<String, Object> tags){
                int key;
                Map.Entry<Integer, Object> tag;
                int[] tagIndices = new int[tags.size()];
                int tagIndex;
                int i = 0;
                for(Map.Entry<String, Object> entry : tags.entrySet()){
                    if(keysMap.containsKey(entry.getKey())){
                        keysMap.put(entry.getKey(), key = keysMap.size());
                    } else {
                        key = keysMap.get(entry.getKey());
                    }

                    tag = new AbstractMap.SimpleEntry<>(key, entry.getValue());

                    if(tagsMap.containsKey(tag)){
                        tagIndex = tagsMap.get(tag);
                    } else {
                        tagsMap.put(tag, tagIndex = tagsMap.size());
                    }
                    tagIndices[i++] = tagIndex;
                }
                return tagIndices;
            }
        }

        public Summary(int index){
            this.index = index;
        }

        public int[] addAlignment(Alignment alignment){

            {
                Integer f = flagsMap.get(alignment.getFlags());
                f = f==null?1:f+1;
                flagsMap.put(alignment.getFlags(), f);
            } {
                Integer l = lenMap.get(alignment.getLength());
                l = l==null?1:l+1;
                    lenMap.put(alignment.getLength(), l);
            } {
                Integer rn = rnextMap.get(alignment.getLength());
                rn = rn==null?1:rn+1;
                rnextMap.put(alignment.getMateReferenceName(), rn);
            }

            return tags.fromTags(alignment.getAttributes());


        }

        public static Summary fromProto(AlignmentProto.Summary summary){
            throw new UnsupportedOperationException();
        }
        public AlignmentProto.Summary toProto( Summary summary){

            return AlignmentProto.Summary.newBuilder()
                    .setDefaultFlag(defaultFlag)
                    .setDefaultLen(defaultLen)
                    .setDefaultOverlapped(defaultOverlapped)
                    .setDefaultRNext(defaultRNext)
                    .setKey(tags.getKeys())
                    .addAllValues(tags.getAllValues())
                    .build();

        }


    }

    public static class AlignmentBucket{

        public AlignmentProto.AlignmentBucket toProto(List<Alignment> alignments){


        }


    }

    public static AlignmentProto.AlignmentBucket toAlignmentBucketProto(List<Alignment> alignments, Summary summary){

    //TODO:
        AlignmentProtoHelper.Summary summary = new AlignmentProtoHelper.Summary();
        int[][] tags = new int[alignments.size()][];

        Iterator<Alignment> iterator = alignments.iterator();
        for(int i = 0; i < alignments.size(); i++){
            tags[i] = summary.addAlignment(iterator.next());
        }

        AlignmentProto.AlignmentBucket.Builder alignmentBucketBuilder = AlignmentProto.AlignmentBucket.newBuilder();

        iterator = alignments.iterator();
        for(int i = 0; i < alignments.size(); i++){
            alignmentBucketBuilder.addAlignmentRecords(AlignmentProtoHelper.toProto(iterator.next(), , ))
        }

        return alignmentBucketBuilder.build();
    }
    public List<Alignment> alignments fromAlignmentBucketProto(AlignmentProto.AlignmentBucket){


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


    /**
     *
     *
     * @param alignment
     * @param chunkStart
     * @return
     */
    public static AlignmentProto.AlignmentRecord toProto(Alignment alignment, long chunkStart){

        AlignmentProto.AlignmentRecord.Builder alignmentRecordBuilder = AlignmentProto.AlignmentRecord.newBuilder()
                .setName(alignment.getName())
                .setFlags(alignment.getFlags())
                .setPos((int) (alignment.getStart() - chunkStart))   //TODO jj: Real Incremental Pos
                .setMapq(alignment.getMappingQuality())
                .setRnext(alignment.getMateReferenceName())
                .setRelativePnext(alignment.getMateAlignmentStart())
                .setQualities(alignment.getQualities())
                .setInferredInsertSize(alignment.getInferredInsertSize())
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

            AlignmentProto.Difference.Builder differenceBuilder = AlignmentProto.Difference.newBuilder()
                    .setOperator(operator)
                    .setPos(alignmentDifference.getPos())
                    .setLength(alignmentDifference.getLength());

            if (alignmentDifference.isSequenceStored()) {
                differenceBuilder.setSequence(ByteString.copyFromUtf8(alignmentDifference.getSeq()));
            }
            alignmentRecordBuilder.addDiffs(differenceBuilder.build());
        }

        AlignmentProto.MyMap.Builder myMapBuilder = AlignmentProto.MyMap.newBuilder();  // alignment attributes
        Map<String, String> tags = alignment.getAttributes();
        if (tags != null) {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                myMapBuilder.addPair(AlignmentProto.MyMap.Pair.newBuilder()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue()));
            }
        }
        alignmentRecordBuilder.setTags(myMapBuilder);

        return alignmentRecordBuilder.build();
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
