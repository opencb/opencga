package org.opencb.opencga.storage.alignment.tasks;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.Alignment.AlignmentDifference;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.commons.run.Task;


/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 11/21/13
 * Time: 5:48 PM
 *
 * Calculates coverage and mean coverage for AlignmentRegion
 *
 **/
public class AlignmentRegionCoverageCalculatorTask extends Task<AlignmentRegion> {

    private class MeanCoverageCalculator {
        private int accumulator;
        private long next;
        private int size;
        List<Float> savedMean;
        private int savedMeanSize;
        private String name;


        public MeanCoverageCalculator(){
            this(1000, "1K");
        }
        public MeanCoverageCalculator(String name){
            int size = 1;
            String numerical;
            char key = name.toUpperCase().charAt(name.length() - 1);    //take last char
            switch (key) {
                case 'G':
                    size *= 1000;
                case 'M':
                    size *= 1000;
                case 'K':
                    size *= 1000;
                    numerical = name.substring(0, name.length() - 1);
                    break;
                default:
                    if (!Character.isDigit(key)) {
                        throw new UnsupportedOperationException("Mean coverage value \"" + name + "\" not supported.");
                    }
                    numerical = name;
                    break;
            }
            size = (int) (size * Float.parseFloat(numerical));
            
            this.accumulator = 0;
            this.next = 0;
            this.size = size;
            this.savedMean = new LinkedList<>();
            this.savedMeanSize = 0;
            this.name = name;
        }
        public MeanCoverageCalculator(int size, String name){
            this.accumulator = 0;
            this.next = 0;
            this.size = size;
            this.savedMean = new LinkedList<>();
            this.savedMeanSize = 0;
            this.name = name;
        }

        public void mean(long position, short all){
            accumulator += all;
            if(next == position){
                //System.out.println("POS: " + position + ", Accumulator = "+ accumulator + " Size: " + size + " Mean= " + ((float)accumulator)/size);
                savedMean.add(savedMeanSize,((float)accumulator)/size);
                savedMeanSize++;
                accumulator = 0;
                next+=size;
            }
        }

        public MeanCoverage takeMeanCoverage(long start){
            MeanCoverage meanCoverage = new MeanCoverage(size,name);
            meanCoverage.setCoverage(takeMeanCoverageArray());
            meanCoverage.setInitPosition((int)(start/size));

            return meanCoverage;
        }

        public float[] takeMeanCoverageArray(){
            float[] array = new float[savedMeanSize];
            for(int i = 0; i < savedMeanSize; i++){
                array[i] = savedMean.get(i);
            }
            savedMeanSize = 0;
            return array;
        }

        public void init(long position){
            this.next =((position)/size+1)*size;
        }
        private void setSize(int size) {
            this.size = size;
        }
        private void setName(String name){
            this.name = name;
        }
    }

    private List<MeanCoverageCalculator> meanCoverageCalculator;

    private long start, end;
    RegionCoverage coverage;
    int  regionCoverageSize;
    long regionCoverageMask;

    private class NativeShortArrayList{
        private short[] array = null;
        private int size = 0;
        private int capacity = 0;

        private NativeShortArrayList(int capacity) {
            this.capacity = capacity;
            this.array = new short[capacity];
        }

        private NativeShortArrayList() {
            this.capacity = 0;
        }

        public void resize(int newSize){
            short[] newArray = new short[newSize];
            for(int i = 0; i < size; i++){
                newArray[i] = array[i];
            }
            array = newArray;
            capacity = newSize;
        }

        public void clear(){
            size = 0;
        }
        public void empty(){
            size = 0;
            array = null;
        }
        public short get(int position){
            return array[position];
        }

        public void add(short elem){
            if(size >= capacity){
                System.out.println("CRASH!");
            }
            if(size >= array.length){
                System.out.println("CRASH!");
            }
            try{
                array[size++] = elem;
            } catch (ArrayIndexOutOfBoundsException e){
                System.out.println("Chrashed...");
            }
        }
        private int size() {
            return size;
        }

        private short[] getArray() {
            return Arrays.copyOfRange(array, 0, size);
        }
        private int getCapacity() {
            return capacity;
        }

    }


    NativeShortArrayList a;
    NativeShortArrayList c;
    NativeShortArrayList g;
    NativeShortArrayList t;
    NativeShortArrayList all;


    int savedSize;


    public AlignmentRegionCoverageCalculatorTask() {
        setRegionCoverageSize(4000);
        a = new   NativeShortArrayList();
        c = new   NativeShortArrayList();
        g = new   NativeShortArrayList();
        t = new   NativeShortArrayList();
        all = new NativeShortArrayList();


        meanCoverageCalculator = new ArrayList<>();

        reset();
//        addMeanCoverageCalculator(1000, "1K");

    }

    public void reset(){
        start = end = 0;
        savedSize = 0;
    }

    /**
     * Uses a circular array to store intermediate values.
     *
     * The start and end values represents the first and the last position stored in the intermediate values.
     *                        d e     f                 g
     *  |---------------------*-*-----*-----------------* end
     *  |                     ^ ^     ^                 ^
     * 1|  ACTGCAGTCGATGTCTATGCTT     |                 |
     * 2|  |   CAGTCGATGTCGATGC       |                 |
     * 3|  |   |       GTCGATGCTTATGCTA                 |
     * 4|  |   |       |                 CCTGCAGTCGATGCTA
     *  |  v   v       v                 v
     *  |--*---*-------*---------------------------------- start
     *     a   b       c                 d
     * Every time the start position represents the start of the last alignment, and the bigger end.
     * Information becomes final when:
     *  -The start of the new alignment is greater than the start.
     *      When the alignment 3 begins to be processed, the region [b,c) will be stored with this.saveCoverage(c)
     *  -The start position of a new alignment is greater than the end
     *      When the alignment 4 begins to be processed, the region [c.d) will be stored
     *  -The AlignmentRegion does not overlap with the next AlignmentRegion.
     *      When all the Alignments are processed, the region [start,end] will be stored
     **/

    @Override
    public boolean apply(List<AlignmentRegion> batch) throws IOException {
        for(AlignmentRegion alignmentRegion : batch){
            if(alignmentRegion == null){
                continue;
            }

            /*
                Initialize
             */
            long coverageStart = start;
            if(start == 0){                 //Set Default value
                coverageStart = start = end = alignmentRegion.getStart();
                for(MeanCoverageCalculator aux : meanCoverageCalculator){
                    aux.init(start);
                }
            }
            int totalSize = (int)(alignmentRegion.getEnd()-alignmentRegion.getStart());
            if(all.getCapacity() < totalSize){
                totalSize*=1.4;
                all.resize(totalSize);
                a.resize(totalSize);
                c.resize(totalSize);
                g.resize(totalSize);
                t.resize(totalSize);
            }
            savedSize = 0;

            /*
                Calculate Coverage
             */
            for(Alignment alignment : alignmentRegion.getAlignments()){
                coverage(alignment);
            }

            if(!alignmentRegion.isOverlapEnd()){
                saveCoverage(alignmentRegion.getEnd()+1);   //[start-end]
                //end = alignmentRegion.getEnd();
                //reset();  // jmml
            }

            /*
                Create Region Coverage  //Todo jcoll: Profile this part
             */
            RegionCoverage regionCoverage = new RegionCoverage();
//            for(int i = 0; i < savedSize; i++){
//                regionCoverage.getA()[i] = a.get(i);
//                regionCoverage.getC()[i] = c.get(i);
//                regionCoverage.getG()[i] = g.get(i);
//                regionCoverage.getT()[i] = t.get(i);
//                regionCoverage.getAll()[i] = all.get(i);
//            }
            regionCoverage.setA(a.getArray());
            regionCoverage.setC(c.getArray());
            regionCoverage.setG(g.getArray());
            regionCoverage.setT(t.getArray());
            regionCoverage.setAll(all.getArray());



//            for(int i = 0; i < savedSize; i++){
//                regionCoverage.getA()[i] = a.get(i);
//            }for(int i = 0; i < savedSize; i++){
//                regionCoverage.getC()[i] = c.get(i);
//            }for(int i = 0; i < savedSize; i++){
//                regionCoverage.getG()[i] = g.get(i);
//            }for(int i = 0; i < savedSize; i++){
//                regionCoverage.getT()[i] = t.get(i);
//            }for(int i = 0; i < savedSize; i++){
//                regionCoverage.getAll()[i] = all.get(i);
//            }
            regionCoverage.setStart(coverageStart);
            regionCoverage.setEnd(coverageStart + savedSize);
            regionCoverage.setChromosome(alignmentRegion.getChromosome());

//            System.out.println(end-coverageStart);
//            System.out.println(start-coverageStart);
//            System.out.println(savedSize);

         //   assert start-coverageStart == savedSize;  //TODO jcoll: Assert this
            alignmentRegion.setCoverage(regionCoverage);
            savedSize = 0;
            a.clear();
            c.clear();
            g.clear();
            t.clear();
            all.clear();

            /*
                Create Mean Coverage List
             */
            List<MeanCoverage> meanCoverageList = new ArrayList<>(meanCoverageCalculator.size());
            for(MeanCoverageCalculator aux: meanCoverageCalculator){
                meanCoverageList.add(aux.takeMeanCoverage(coverageStart));
            }
            alignmentRegion.setMeanCoverage(meanCoverageList);

            if(!alignmentRegion.isOverlapEnd()){
                end = alignmentRegion.getEnd();
                reset();
            }
        }
        return true;
    }

    private void saveCoverage(long endP){
        //Saves the actual coverage from start to end
        int pos;
        for(long i = start; i < endP; i++){
            pos = (int)(i & regionCoverageMask);

            a.add(/*savedSize,*/ coverage.getA()[pos]);
            coverage.getA()[pos] = 0;

            c.add(/*savedSize, */coverage.getC()[pos]);
            coverage.getC()[pos] = 0;

            g.add(/*savedSize, */coverage.getG()[pos]);
            coverage.getG()[pos] = 0;

            t.add(/*savedSize, */coverage.getT()[pos]);
            coverage.getT()[pos] = 0;

            all.add(/*savedSize, */coverage.getAll()[pos]);
            
            for(MeanCoverageCalculator aux : meanCoverageCalculator){
                aux.mean(i,coverage.getAll()[pos]);
            }
            coverage.getAll()[pos] = 0;
            savedSize++;
        }
        start = endP;
    }

    private int coverage(Alignment alignment){
        if((alignment.getFlags() & Alignment.SEGMENT_UNMAPPED) != 0){
            return 0;
        }
        if(alignment.getLength() > regionCoverageSize){
            setRegionCoverageSize(alignment.getLength());
        }
        if(alignment.getStart() > end){
            saveCoverage(end+1);  //Save to the end
            saveCoverage(alignment.getStart());  //Save zeros to the start
            //System.out.println(alignment.getStart());
        } else {
            saveCoverage(alignment.getStart());
        }
        start = alignment.getStart();
        if(alignment.getEnd()>end){
            end = alignment.getEnd();
        }
        //byte[] sequence = alignment.getReadSequence();
        String seq;

        //Iterator<AlignmentDifference> diferencesIterator = alignment.getDifferences().iterator();
        //AlignmentDifference alignmentDifference = diferencesIterator.hasNext()? diferencesIterator.next():null;
        int offset = 0; // offset caused by insertions and deletions
        int clipping = 0; 
        
        //int validBases = (int)(alignment.getEnd()-alignment.getStart());
        int pos = 0;
        for(AlignmentDifference diff : alignment.getDifferences()){
            for(; pos + clipping < diff.getPos(); pos++){
                    coverage.getAll()[(int) ((pos + start) & regionCoverageMask)]++;
            }
            switch(diff.getOp()){
                case AlignmentDifference.INSERTION:
                    //i += alignmentDifference.getLength();
                    offset -= diff.getLength();
                    break;
                case AlignmentDifference.DELETION:
                    pos += diff.getLength();
                    offset += diff.getLength();
                    break;
                case AlignmentDifference.SOFT_CLIPPING:
                    //pos += diff.getLength();
                    clipping += diff.getLength();
                case AlignmentDifference.SKIPPED_REGION:
                case AlignmentDifference.HARD_CLIPPING:
                case AlignmentDifference.PADDING:
                default:
                    break;
                case AlignmentDifference.MATCH_MISMATCH:
                case AlignmentDifference.MISMATCH: {
                    seq = diff.getSeq();
                    if(seq != null){
                        for(char c : seq.toCharArray()){
                            switch(c){
                                case 'A':
                                    coverage.getA()[(int) ((pos + start) & regionCoverageMask)]++;
                                    break;
                                case 'C':
                                    coverage.getC()[(int) ((pos + start) & regionCoverageMask)]++;
                                    break;
                                case 'G':
                                    coverage.getG()[(int) ((pos + start) & regionCoverageMask)]++;
                                    break;
                                case 'T':
                                    coverage.getT()[(int) ((pos + start) & regionCoverageMask)]++;
                                    break;
                                default:
                                    break;
                            }
                            coverage.getAll()[(int) ((pos + start) & regionCoverageMask)]++;
                            pos++;
                        }
                    }   //else, in the next loop will increase the "all" coverage
                    
                    break;
                }
            }
        }
        for (; pos + clipping - offset < alignment.getLength() ; pos++) {
            coverage.getAll()[(int) ((pos + start) & regionCoverageMask)]++;
        }
        //assert pos == validBases;
        if(pos + clipping - offset != alignment.getLength()){
            System.out.println("Nooooo");
        }
        //assert pos+clipping == alignment.getLength();
        
//        for(int i = 0; i < alignment.getLength(); i++) {
//            //        for(int i = 0; i < sequence.length; i++) {    //TODO jcoll: Analyze this case
//            assert alignment.getLength() == sequence.length;
//            if (alignmentDifference != null) {  // if there are remaining differences
//                if(alignmentDifference.getPos() == i) {
//                    switch(alignmentDifference.getOp()){
//                        case Alignment.AlignmentDifference.INSERTION:
//                            i += alignmentDifference.getLength();
//                            offset -= alignmentDifference.getLength();
//                            break;
//                        case Alignment.AlignmentDifference.DELETION:
//                            offset += alignmentDifference.getLength();
//                            break;
//                        case Alignment.AlignmentDifference.MISMATCH:
//                        case Alignment.AlignmentDifference.SKIPPED_REGION:
//                        case Alignment.AlignmentDifference.SOFT_CLIPPING:
//                        case Alignment.AlignmentDifference.HARD_CLIPPING:
//                        case Alignment.AlignmentDifference.PADDING:
//                        default:
//                            break;
//                    }
//                    if (diferencesIterator.hasNext()) {
//                        alignmentDifference = diferencesIterator.next();
//                    } else {
//                        alignmentDifference = null;
//                    }
//                }
//            }
//            if(i < alignment.getLength()){ //TODO jj: Write a correct commentary
//                switch (sequence[i]) {
//                    case 'A':
//                        coverage.getA()[(int) ((i + offset + start) & regionCoverageMask)]++;
//                        break;
//                    case 'C':
//                        coverage.getC()[(int) ((i + offset + start) & regionCoverageMask)]++;
//                        break;
//                    case 'G':
//                        coverage.getG()[(int) ((i + offset + start) & regionCoverageMask)]++;
//                        break;
//                    case 'T':
//                        coverage.getT()[(int) ((i + offset + start) & regionCoverageMask)]++;
//                        break;
//                    default:
//                        //TODO jcoll: Analyze this case
//                        break;
//                }
//            }
//        }

        return 0;
    }

    /**
     * Set size to the nearest upper 2^n number for quick modulus operation
     *
     * @param size
     */
    public void setRegionCoverageSize(int size){
        if(size < 0){
            return;
        }
        int lg = (int)Math.ceil(Math.log(size)/Math.log(2));
        //int lg = 31 - Integer.numberOfLeadingZeros(size);
        int newRegionCoverageSize = 1 << lg;
        int newRegionCoverageMask = newRegionCoverageSize - 1;
        RegionCoverage newCoverage = new RegionCoverage(newRegionCoverageSize);

        if(coverage != null){
            for(int i = 0; i < (end-start); i++){
                newCoverage.getA()[(int)((start+i)&newRegionCoverageMask)] = coverage.getA()[(int)((start+i)&regionCoverageMask)];
            }
        }

        regionCoverageSize = newRegionCoverageSize;
        regionCoverageMask = newRegionCoverageMask;
        coverage = newCoverage;
//        System.out.println("Region Coverage Mask : " + regionCoverageMask);
    }

    public void addMeanCoverageCalculator(int size, String name) {
        this.meanCoverageCalculator.add(new MeanCoverageCalculator(size, name));
    }

    public void addMeanCoverageCalculator(String name) {
        this.meanCoverageCalculator.add(new MeanCoverageCalculator(name));
    }


}
