/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.alignment.tasks;


import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.Alignment.AlignmentDifference;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.run.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


/**
 * Date: 11/21/13.
 *
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 *         <p>
 *         Calculates coverage and mean coverage for AlignmentRegion
 **/
public class AlignmentRegionCoverageCalculatorTask extends Task<AlignmentRegion> {

    /**
     * Calculates the Mean Coverage at intervals.
     */
    private class MeanCoverageCalculator {
        private final int size;
        private final String name;

        private int accumulator;
        private long next;

        public MeanCoverageCalculator(String name) {
            this.accumulator = 0;
            this.next = 0;
            this.size = MeanCoverage.nameToSizeConvert(name);
            this.name = name;
        }

        public MeanCoverageCalculator(int size, String name) {
            this.accumulator = 0;
            this.next = 0;
            this.size = size;
            this.name = name;
        }


        public List<MeanCoverage> calculateMeanCoverage(RegionCoverage coverage) {
            List<MeanCoverage> list = new LinkedList<>();
            final short[] all = coverage.getAll();

            if (coverage.getStart() >= next) {
                Region region = new Region(coverage.getChromosome(), (int) next - size, (int) next - 1);
                list.add(new MeanCoverage(size, name, region, (float) accumulator / size));
                reset(coverage.getStart());
            }

            int i = 0;
            boolean lastIteration = false;
            while (i < all.length) {
                int lim = (int) (next - coverage.getStart());
                if (all.length < lim) {
                    lim = all.length;
                    lastIteration = true;
                }
                for (; i < lim; i++) {
                    accumulator += all[i];
                }
                if (!lastIteration) {    //The last iteration will keep the defaultValue for the next call to this function
                    Region region = new Region(coverage.getChromosome(), (int) next - size, (int) next - 1);
                    list.add(new MeanCoverage(size, name, region, (float) accumulator / size));
                    next += size;
                    accumulator = 0;
                }
            }

            return list;
        }

        public void reset(long position) {
            this.next = ((position - 1) / size + 1) * size + 1;  //Calculates the NEXT interval starting position
            this.accumulator = 0;                   //Reset the accumulator
        }

    }

    private final class NativeShortArrayList {
        private short[] array = new short[0];
        private int size = 0;
        private int capacity = 0;

        private NativeShortArrayList(int capacity) {
            this.capacity = capacity;
            this.array = new short[capacity];
        }

        private NativeShortArrayList() {
            this.capacity = 0;
        }

        public void resize(int newSize) {
            array = Arrays.copyOf(array, newSize);
            capacity = newSize;
        }

        public void clear() {
            size = 0;
        }

        public void empty() {
            size = 0;
            array = null;
        }

        public short get(int position) {
            return array[position];
        }

        public void add(short elem) {
            if (size >= capacity) {
                resize(capacity * 2);
            }
            array[size++] = elem;
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

    private List<MeanCoverageCalculator> meanCoverageCalculator;

    private long start = 0;
    private long end = 0;
    private RegionCoverage coverage;
    private int regionCoverageSize;
    private long regionCoverageMask;

    private NativeShortArrayList a;
    private NativeShortArrayList c;
    private NativeShortArrayList g;
    private NativeShortArrayList t;
    private NativeShortArrayList all;

    private int savedSize;


    public AlignmentRegionCoverageCalculatorTask() {
        setRegionCoverageSize(4000);
        a = new NativeShortArrayList();
        c = new NativeShortArrayList();
        g = new NativeShortArrayList();
        t = new NativeShortArrayList();
        all = new NativeShortArrayList();

        meanCoverageCalculator = new ArrayList<>();

        reset();
    }

    public void reset() {
        start = 0;
        end = 0;
        savedSize = 0;
    }

    /**
     * Uses a circular array to store intermediate values.
     * <p>
     * The start and end values represents the first and the last position stored in the intermediate values.
     * d e     f                 g
     * |---------------------*-*-----*-----------------* end
     * |                     ^ ^     ^                 ^
     * 1|  ACTGCAGTCGATGTCTATGCTT     |                 |
     * 2|  |   CAGTCGATGTCGATGC       |                 |
     * 3|  |   |       GTCGATGCTTATGCTA                 |
     * 4|  |   |       |                 CCTGCAGTCGATGCTA
     * |  v   v       v                 v
     * |--*---*-------*---------------------------------- start
     * a   b       c                 d
     * Every time the start position represents the start of the last alignment, and the bigger end.
     * Information becomes final when:
     * -The start of the new alignment is greater than the start.
     * When the alignment 3 begins to be processed, the region [b,c) will be stored with this.saveCoverage(c)
     * -The start position of a new alignment is greater than the end
     * When the alignment 4 begins to be processed, the region [c.d) will be stored
     * -The AlignmentRegion does not overlap with the next AlignmentRegion.
     * When all the Alignments are processed, the region [start,end] will be stored
     **/

    @Override
    public boolean apply(List<AlignmentRegion> batch) throws IOException {
        for (AlignmentRegion alignmentRegion : batch) {
            if (alignmentRegion == null) {
                continue;
            }

            /*
                Initialize
             */
            long coverageStart = start;
            if (start == 0) {                 //Set Default defaultValue
                coverageStart = alignmentRegion.getStart();
                start = alignmentRegion.getStart();
                end = alignmentRegion.getStart();
                for (MeanCoverageCalculator aux : meanCoverageCalculator) {
                    aux.reset(start);
                }
            }
            int totalSize = (int) (alignmentRegion.getEnd() - alignmentRegion.getStart());
            if (all.getCapacity() < totalSize) {
                totalSize *= 1.4;
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
            for (Alignment alignment : alignmentRegion.getAlignments()) {
                coverage(alignment);
            }

            if (!alignmentRegion.isOverlapEnd()) {
                saveCoverage(alignmentRegion.getEnd() + 1);   //[start-end]
            }

            /*
                Create Region Coverage  //Todo jcoll: Profile this part
             */
            RegionCoverage regionCoverage = new RegionCoverage();

            regionCoverage.setA(a.getArray());
            regionCoverage.setC(c.getArray());
            regionCoverage.setG(g.getArray());
            regionCoverage.setT(t.getArray());
            regionCoverage.setAll(all.getArray());


            regionCoverage.setStart(coverageStart);
            regionCoverage.setEnd(coverageStart + savedSize);
            regionCoverage.setChromosome(alignmentRegion.getChromosome());


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
            for (MeanCoverageCalculator aux : meanCoverageCalculator) {
                meanCoverageList.addAll(aux.calculateMeanCoverage(regionCoverage));
            }
            alignmentRegion.setMeanCoverage(meanCoverageList);

            if (!alignmentRegion.isOverlapEnd()) {
                end = alignmentRegion.getEnd();
                reset();
            }
        }
        return true;
    }

    private void saveCoverage(long endP) {
        //Saves the actual coverage from start to end
        int pos;
        for (long i = start; i < endP; i++) {
            pos = (int) (i & regionCoverageMask);

            a.add(/*savedSize,*/ coverage.getA()[pos]);
            coverage.getA()[pos] = 0;

            c.add(/*savedSize, */coverage.getC()[pos]);
            coverage.getC()[pos] = 0;

            g.add(/*savedSize, */coverage.getG()[pos]);
            coverage.getG()[pos] = 0;

            t.add(/*savedSize, */coverage.getT()[pos]);
            coverage.getT()[pos] = 0;

            all.add(/*savedSize, */coverage.getAll()[pos]);

            coverage.getAll()[pos] = 0;
            savedSize++;
        }
        start = endP;
    }

    private int coverage(Alignment alignment) {
        if ((alignment.getFlags() & Alignment.SEGMENT_UNMAPPED) != 0) {
            return 0;
        }
        if (alignment.getLength() > regionCoverageSize) {
            setRegionCoverageSize(alignment.getLength());
        }
        if (alignment.getStart() > end) {
            saveCoverage(end + 1);  //Save to the end
            saveCoverage(alignment.getStart());  //Save zeros to the start
            //System.out.println(alignment.getStart());
        } else {
            saveCoverage(alignment.getStart());
        }
        start = alignment.getStart();
        if (alignment.getEnd() > end) {
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
        for (AlignmentDifference diff : alignment.getDifferences()) {
            for (; pos + clipping < diff.getPos(); pos++) {
                coverage.getAll()[(int) ((pos + start) & regionCoverageMask)]++;
            }
            switch (diff.getOp()) {
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
                case AlignmentDifference.MATCH_MISMATCH:
                case AlignmentDifference.MISMATCH: {
                    seq = diff.getSeq();
                    if (seq != null) {
                        for (char c : seq.toCharArray()) {
                            switch (c) {
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
                default:
                    break;
            }
        }
        for (; pos + clipping - offset < alignment.getLength(); pos++) {
            coverage.getAll()[(int) ((pos + start) & regionCoverageMask)]++;
        }
        //assert pos == validBases;
        if (pos + clipping - offset != alignment.getLength()) {
            System.out.println("[ERROR] assert pos == validBases");   //TODO jcoll: Assert this
        }
        //assert pos+clipping == alignment.getLength();

        return 0;
    }

    /**
     * Set size to the nearest upper 2^n number for quick modulus operation.
     *
     * @param size Region size in bp
     */
    public void setRegionCoverageSize(int size) {
        if (size < 0) {
            return;
        }
        int lg = (int) Math.ceil(Math.log(size) / Math.log(2));
        //int lg = 31 - Integer.numberOfLeadingZeros(size);
        int newRegionCoverageSize = 1 << lg;
        int newRegionCoverageMask = newRegionCoverageSize - 1;
        RegionCoverage newCoverage = new RegionCoverage(newRegionCoverageSize);

        if (coverage != null) {
            for (int i = 0; i < (end - start); i++) {
                newCoverage.getA()[(int) ((start + i) & newRegionCoverageMask)] = coverage.getA()[(int) ((start + i) & regionCoverageMask)];
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
