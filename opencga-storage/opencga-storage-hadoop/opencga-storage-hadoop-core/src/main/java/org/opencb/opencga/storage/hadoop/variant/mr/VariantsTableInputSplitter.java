package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created on 26/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantsTableInputSplitter {

    /**
     * Align each InputSplit to an specified frame size.
     * Moves each start-end of each TableSplit.
     *
     * @param inputSplits List of TableSplits to adjust. This will not be modified
     * @param frameSize   Frame size to adjust
     * @return List of modified TableSplits
     * @throws IOException When reading the scan instance from the TableSplit fails.
     */
    public static List<InputSplit> alignInputSplits(List<InputSplit> inputSplits, int frameSize) throws IOException {
        // Copy list
        inputSplits = new LinkedList<>(inputSplits);
        ListIterator<InputSplit> it = inputSplits.listIterator();

        while (it.hasNext()) {
            TableSplit split = (TableSplit) it.next();
            TableSplit alignedSplit = alignTableSplit(split, frameSize);
            if (alignedSplit == null) {
                it.remove();
            } else {
                it.set(alignedSplit);
            }
        }

        return inputSplits;
    }

    /**
     * Align given TableSplit to an specific frame size.
     * <p>
     * If the split is too small to fit in a frame, it is not valid, and it should be removed.
     *
     * @param tableSplit Input TableSplit. This will not be modified
     * @param frameSize  Frame size to adjust
     * @return The new adjusted TableSplit. Null if the Split should be discarded.
     * @throws IOException When reading the scan instance from the TableSplit fails.
     */
    private static TableSplit alignTableSplit(TableSplit tableSplit, int frameSize) throws IOException {
        Pair<String, Integer> start;
        Pair<String, Integer> end;
        int newStartPos;
        int newEndPos;
        byte[] newStartRow;
        byte[] newEndRow;
        if (!ArrayUtils.isEmpty(tableSplit.getStartRow())) {
            start = VariantPhoenixKeyFactory
                    .extractChrPosFromVariantRowKey(tableSplit.getStartRow(), 0, tableSplit.getStartRow().length, true);
            newStartPos = start.getSecond() - (start.getSecond() % frameSize);
            newStartRow = VariantPhoenixKeyFactory.generateVariantRowKey(start.getFirst(), newStartPos);
//            System.err.println("Move start " + printPair(start) + "->" + newStartPos);
        } else {
            start = null;
            newStartPos = 0;
            newStartRow = tableSplit.getStartRow();
        }
        if (!ArrayUtils.isEmpty(tableSplit.getEndRow())) {
            end = VariantPhoenixKeyFactory
                    .extractChrPosFromVariantRowKey(tableSplit.getEndRow(), 0, tableSplit.getEndRow().length, true);
            newEndPos = end.getSecond() - (end.getSecond() % frameSize);
            newEndRow = VariantPhoenixKeyFactory.generateVariantRowKey(end.getFirst(), newEndPos);
//            System.err.println("Move end " + printPair(end) + "->" + newEndPos);
        } else {
            end = null;
            newEndPos = 0;
            newEndRow = tableSplit.getEndRow();
        }

        if (start != null && end != null && start.getFirst().equals(end.getFirst())) {
            // Region within the same chromosome
            if (newStartPos == newEndPos) {
                // Region too small. Invalid split, should be removed.
//                System.err.println("Discard split " + printPair(start) + "->" + printPair(end));
                return null;
            }
        }

        Scan scan = tableSplit.getScan();

        scan.setStartRow(newStartRow);
        scan.setStopRow(newEndRow);


        return new TableSplit(tableSplit.getTable(), scan, newStartRow, newEndRow, tableSplit.getRegionLocation(), tableSplit.getLength());
    }

    private static String printPair(Pair<String, Integer> pair) {
        return pair == null ? "null" : (pair.getFirst() + ":" + pair.getSecond());
    }

}
