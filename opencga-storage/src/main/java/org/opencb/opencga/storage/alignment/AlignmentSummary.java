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

package org.opencb.opencga.storage.alignment;

import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.opencga.storage.alignment.proto.AlignmentProto;

import java.util.*;

/*
 * TODO: Make a builder
 */

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 4/7/14
 * Time: 7:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentSummary {
    //Only can add an Alignment when it's opened.
    private boolean open;
    final private int index;

    private int defaultFlag;
    private int defaultLen;
    private String defaultRNext;
    private int defaultOverlappedBucket;
    private String[] keysArray;
    private Map<String, Integer> keysMap;
    private Map.Entry<Integer, Object>[] tagsArray;
    private Map<Map.Entry<Integer, Object>, Integer> tagsMap;


    public static class AlignmentRegionSummaryBuilder {
        final private AlignmentSummary summary;

        //Histogram for default values.
        final private Map<Integer, Integer> flagsMap;
        final private Map<Integer, Integer> lenMap;
        final private Map<String, Integer> rnextMap;
        final private Map<Integer, Integer> overlapMap;

        public AlignmentRegionSummaryBuilder(int index) {
            summary = new AlignmentSummary(index);

            this.flagsMap = new HashMap<>();
            this.lenMap = new HashMap<>();
            this.rnextMap = new HashMap<>();
            this.overlapMap = new HashMap<>();
        }

        /**
         * This function can only be called when summary is OPEN.
         *
         * @param overlapped
         * @return builder
         */
        public AlignmentRegionSummaryBuilder addOverlappedBucket(int overlapped) {
            Integer f = overlapMap.get(overlapped);
            f = f == null ? 1 : f + 1;
            overlapMap.put(overlapped, f);
            return this;
        }

        /**
         * This function can only be called when summary is OPEN.
         *
         * @param alignment
         * @return builder
         */
        public AlignmentRegionSummaryBuilder addAlignment(Alignment alignment) {
            if (!summary.open) {
                System.out.println("[ERROR] Alignmentregionsummary.addAlignment() can't be called when is closed!");
                //TODO jj: Throw exception?
                return this;
            }
            //FLAG
            {
                Integer f = flagsMap.get(alignment.getFlags());
                f = f == null ? 1 : f + 1;
                flagsMap.put(alignment.getFlags(), f);
            }

            //LENGTH
            {
                Integer l = lenMap.get(alignment.getLength());
                l = l == null ? 1 : l + 1;
                lenMap.put(alignment.getLength(), l);
            }

            //MateReferenceName
            {
                Integer rn = rnextMap.get(alignment.getMateReferenceName());
                rn = rn == null ? 1 : rn + 1;
                rnextMap.put(alignment.getMateReferenceName(), rn);
            }

            //Tags / Attributes
            int key;
            Map.Entry<Integer, Object> tag;
            for (Map.Entry<String, Object> entry : alignment.getAttributes().entrySet()) {

                //Update key map
                if (!summary.keysMap.containsKey(entry.getKey())) {
                    summary.keysMap.put(entry.getKey(), key = summary.keysMap.size());
                } else {
                    key = summary.keysMap.get(entry.getKey());
                }

                //Add new map
                tag = new HashMap.SimpleEntry<>(key, entry.getValue());

                if (!summary.tagsMap.containsKey(tag)) {
                    summary.tagsMap.put(tag, summary.tagsMap.size());
                }
            }
            return this;
        }

        public AlignmentSummary build() {
            if (summary.open == false) {
                return summary;
            }
            int maxFlags = 0;
            for (Map.Entry<Integer, Integer> entry : flagsMap.entrySet()) {
                if (entry.getValue() > maxFlags) {
                    maxFlags = entry.getValue();
                    summary.defaultFlag = entry.getKey();
                }
            }

            int maxLen = 0;
            for (Map.Entry<Integer, Integer> entry : lenMap.entrySet()) {
                if (entry.getValue() > maxLen) {
                    maxLen = entry.getValue();
                    summary.defaultLen = entry.getKey();
                }
            }

            int maxRNext = 0;
            for (Map.Entry<String, Integer> entry : rnextMap.entrySet()) {
                if (entry.getValue() > maxRNext) {
                    maxRNext = entry.getValue();
                    summary.defaultRNext = entry.getKey();
                }
            }

            int maxOverlap = 0;
            for (Map.Entry<Integer, Integer> entry : overlapMap.entrySet()) {
                if (entry.getValue() > maxOverlap) {
                    maxOverlap = entry.getValue();
                    summary.defaultOverlappedBucket = entry.getKey();
                }
            }

            summary.keysArray = new String[summary.keysMap.size()];
            for (Map.Entry<String, Integer> entry : summary.keysMap.entrySet()) {
                summary.keysArray[entry.getValue()] = entry.getKey();
            }

            summary.tagsArray = new Map.Entry[summary.tagsMap.size()];
            for (Map.Entry<Map.Entry<Integer, Object>, Integer> entry : summary.tagsMap.entrySet()) {
                summary.tagsArray[entry.getValue()] = entry.getKey();
            }

            summary.open = false;

            return summary;
        }

        public void printHistogram() {
            System.out.println("AlignmentRegionSummary [" + summary.index + "] Histogram");
            System.out.println("Default Flag Map");
            for (Map.Entry<Integer, Integer> entry : flagsMap.entrySet()) {
                System.out.print(entry.getKey() + "\t");
                for (int i = 0; i < entry.getValue(); i++) System.out.print("*");
                System.out.println("");
            }

            System.out.println("\nDefault Length Map");
            for (Map.Entry<Integer, Integer> entry : lenMap.entrySet()) {
                System.out.print(entry.getKey() + "\t");
                for (int i = 0; i < entry.getValue(); i++) System.out.print("*");
                System.out.println("");
            }

            System.out.println("\nDefault RNext Map");
            for (Map.Entry<String, Integer> entry : rnextMap.entrySet()) {
                System.out.print(entry.getKey() + "\t");
                for (int i = 0; i < entry.getValue(); i++) System.out.print("*");
                System.out.println("");
            }

            System.out.println("\nDefault OverlappedBucket Map");
            for (Map.Entry<Integer, Integer> entry : overlapMap.entrySet()) {
                System.out.print(entry.getKey() + "\t");
                for (int i = 0; i < entry.getValue(); i++) System.out.print("*");
                System.out.println("");
            }
        }

    }

    private AlignmentSummary(int index) {
        this.open = true;
        this.index = index;

        this.keysMap = new HashMap<>();
        this.keysArray = null;
        this.tagsMap = new HashMap<>();
        this.tagsArray = null;

    }

    public AlignmentSummary(AlignmentProto.Summary summary, int index) {
        this.open = false;

        this.index = index;

        this.defaultFlag = summary.getDefaultFlag();
        this.defaultLen = summary.getDefaultLen();
        this.defaultRNext = summary.getDefaultRNext();
        this.defaultOverlappedBucket = summary.getDefaultOverlapped();

        String keys = summary.getKeys();
        this.keysArray = new String[keys.length() / 2];
        this.keysMap = new HashMap<>();
        for (int i = 0; i < keys.length() / 2; i++) {
            keysArray[i] = keys.substring(i * 2, i * 2 + 2);
            keysMap.put(keysArray[i], i);
        }

        Map.Entry<Integer, Object> tag;
        Object value = "";
        this.tagsMap = new HashMap<>();
        this.tagsArray = new Map.Entry[summary.getValuesCount()];
        for (AlignmentProto.Summary.Pair pair : summary.getValuesList()) {
            if (pair.hasAvalue()) {
                value = (char) pair.getAvalue();
            } else if (pair.hasFvalue()) {
                value = pair.getFvalue();
            } else if (pair.hasIvalue()) {
                value = pair.getIvalue();
            } else if (pair.hasZvalue()) {
                value = pair.getZvalue();
            }
            AbstractMap.SimpleEntry<Integer, Object> entry = new HashMap.SimpleEntry<>(pair.getKey(), value);
            tagsArray[tagsMap.size()] = entry;
            tagsMap.put(entry, tagsMap.size());
        }

    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public AlignmentProto.Summary toProto() {

        String keys = "";

        for (String s : keysArray) {
            keys += s;
        }

        AlignmentProto.Summary.Pair[] pairsArray = new AlignmentProto.Summary.Pair[tagsMap.size()];
        for (Map.Entry<Map.Entry<Integer, Object>, Integer> entry : tagsMap.entrySet()) {
            if (pairsArray[entry.getValue()] != null) {
                System.out.println("[ERROR] Duplicated tag index.");
            }

            AlignmentProto.Summary.Pair.Builder builder = AlignmentProto.Summary.Pair.newBuilder();
            if (entry.getKey().getValue() instanceof Integer) {
                builder.setIvalue((Integer) entry.getKey().getValue());
            } else if (entry.getKey().getValue() instanceof Float) {
                builder.setFvalue((Float) entry.getKey().getValue());
            } else if (entry.getKey().getValue() instanceof Character) {
                builder.setAvalue((Character) entry.getKey().getValue());
            } else { //if (entry.getKey().getValue() instanceof String) {
                builder.setZvalue((String) entry.getKey().getValue());
            }

            pairsArray[entry.getValue()] = builder
                    .setKey(entry.getKey().getKey())
                    .build();

        }

        return AlignmentProto.Summary.newBuilder()
                .setDefaultFlag(defaultFlag)
                .setDefaultLen(defaultLen)
                .setDefaultOverlapped(defaultOverlappedBucket)
                .setDefaultRNext(defaultRNext)
                .setKeys(keys)
                .addAllValues(Arrays.asList(pairsArray))
                .build();

    }

    /**
     * This function can only be called when summary is CLOSED.
     * <p>
     * TODO jj: Add better algorithm. For 2.0
     */
    public ArrayList<Integer> getIndexTagList(Map<String, Object> alignmentTags) {
        ArrayList<Integer> indexTagList = new ArrayList<>(alignmentTags.size());

        int key;
        Map.Entry<Integer, Object> tag;
        for (Map.Entry<String, Object> entry : alignmentTags.entrySet()) {
            key = keysMap.get(entry.getKey());
            tag = new HashMap.SimpleEntry<>(key, entry.getValue());
            indexTagList.add(tagsMap.get(tag));
        }

        return indexTagList;
    }

    public Map<String, Object> getTagsFromList(List<Integer> indexTagList) {
        Map<String, Object> tags = new HashMap<>();

        for (Integer i : indexTagList) {
            try {
                tags.put(keysArray[tagsArray[i].getKey()], tagsArray[i].getValue());
            } catch (ArrayIndexOutOfBoundsException ex) {
                System.out.println("Error? summary.getTagsFromList()");
            }
        }

        return tags;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public int getDefaultOverlapped() {
        return defaultOverlappedBucket;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public String getDefaultRNext() {
        return defaultRNext;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public int getDefaultLen() {
        return defaultLen;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public int getDefaultFlag() {
        return defaultFlag;
    }

    /**
     * This function can only be called when summary is CLOSED.
     */
    public int getIndex() {
        return index;
    }

}
