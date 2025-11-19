package org.opencb.opencga.storage.core.utils;

import java.util.*;
import java.util.function.BiFunction;

public class GenomeSplitFactory {

    /**
     * Generate a list of splits for the human genome. The n splits will define n+1 regions with approximately equal size.
     *
     * TODO: Query CellBase to get the chromosomes and sizes!
     * @param numberOfSplits    Number of split points.
     * @return                  List of splits
     */
    public static List<String> generateBootPreSplitsHuman(int numberOfSplits) {
        return generateBootPreSplitsHuman(numberOfSplits, (chr, pos) -> chr + ":" + pos, String::compareTo, false);
    }

    /**
     * Generate a list of splits for the human genome. The n splits will define n+1 regions with approximately equal size.
     *
     * TODO: Query CellBase to get the chromosomes and sizes!
     * @param numberOfSplits    Number of split points.
     * @param keyGenerator      Function to generate a key given a chromosome and a start
     * @param compareTo         Comparator to sort the keys
     * @param includeEndSplit   Include the last split point, which is the end of the last chromosome.
     * @param <T>               Type of the split
     * @return                  List of splits
     */
    public static <T> List<T> generateBootPreSplitsHuman(int numberOfSplits, BiFunction<String, Integer, T> keyGenerator,
                                                         Comparator<T> compareTo, boolean includeEndSplit) {
        String[] chr = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20", "21", "22", "X", "Y", };
        long[] posarr = new long[]{249250621, 243199373, 198022430, 191154276, 180915260, 171115067, 159138663,
                146364022, 141213431, 135534747, 135006516, 133851895, 115169878, 107349540, 102531392, 90354753,
                81195210, 78077248, 59128983, 63025520, 48129895, 51304566, 155270560, 59373566, };
        Map<String, Long> regions = new HashMap<>();
        for (int i = 0; i < chr.length; i++) {
            regions.put(chr[i], posarr[i]);
        }
        return generateBootPreSplits(numberOfSplits, keyGenerator, regions, compareTo, includeEndSplit);
    }

    static <T> List<T> generateBootPreSplits(int numberOfSplits, BiFunction<String, Integer, T> keyGenerator,
                                             Map<String, Long> regionsMap, Comparator<T> comparator, boolean includeEndSplit) {
        // Create a sorted map for the regions that sorts as will sort HBase given the row_key generator
        // In archive table, chr1 goes after chr19, and in Variants table, chr1 is always the first
        SortedMap<String, Long> sortedRegions = new TreeMap<>((s1, s2) ->
                comparator.compare(keyGenerator.apply(s1, 0), keyGenerator.apply(s2, 0)));
        sortedRegions.putAll(regionsMap);

        long total = regionsMap.values().stream().mapToLong(Long::longValue).sum();
        long chunkSize = total / numberOfSplits;
        List<T> splitList = new ArrayList<>();
        long splitPos = chunkSize;
        while (splitPos < total) {
            long tmpPos = 0;
            String chr = null;

            for (Map.Entry<String, Long> entry : sortedRegions.entrySet()) {
                long v = entry.getValue();
                if ((tmpPos + v) > splitPos) {
                    chr = entry.getKey();
                    break;
                }
                tmpPos += v;
            }
            T rowKey = keyGenerator.apply(chr, (int) (splitPos - tmpPos));
            splitList.add(rowKey);
            splitPos += chunkSize;
        }
        // End split is always added, unless the numberOfSplits is a multiple of the "total" size.
        boolean hasEndSplit = splitList.size() == numberOfSplits;
        if (includeEndSplit) {
            if (!hasEndSplit) {
                // Add last split
                String lastKey = sortedRegions.lastKey();
                splitList.add(keyGenerator.apply(lastKey, sortedRegions.get(lastKey).intValue()));
            }
        } else {
            if (hasEndSplit) {
                // Remove last split
                splitList.remove(splitList.size() - 1);
            }
        }
        return splitList;
    }
}
