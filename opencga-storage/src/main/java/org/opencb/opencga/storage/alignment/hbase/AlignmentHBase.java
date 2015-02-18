/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.opencb.opencga.storage.alignment.hbase;

/**
 *
 * @author jacobo
 */
public class AlignmentHBase {

    public static final int ALIGNMENT_BUCKET_SIZE = 2048;
    public static final String ALIGNMENT_COLUMN_FAMILY_NAME = "a";
    public static final String ALIGNMENT_COVERAGE_COLUMN_FAMILY_NAME = "c";
    
    public static String getChromosomeFromRowkey(String rowKey) {
        return rowKey.split("_")[0];
    }

    public static String getBucketRowkey(String chromosome, long start, int bucketSize) {
        return getBucketRowkey(chromosome, start / bucketSize);
    }

    public static String getBucketRowkey(String chromosome, long bucketIndex) {
        return chromosome + "_" + String.format("%07d", bucketIndex);
    }

    public static String getSummaryRowkey(String chromosome, int index) {
        return "S_" + chromosome + "_" + String.format("%05d", index);
    }

    public static long getPositionFromRowkey(String rowKey, int bucketSize) {
        return Long.valueOf(rowKey.split("_")[1]) * bucketSize;
    }
    
    public static String getMeanCoverageRowKey(String chromosome, String coverageName, int coverageIndex){
        return chromosome + "_" + coverageName + "_" + String.format("%08d", coverageIndex);
    }

    static String getHeaderRowKey() {
        return "summary";
    }
    
    
}
