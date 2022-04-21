/*
 * Copyright 2015-2017 OpenCB
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

/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant;

import com.google.protobuf.MessageLite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.*;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class GenomeHelper {
    private final Logger logger = LoggerFactory.getLogger(GenomeHelper.class);

    // MUST BE UPPER CASE!!!
    // Phoenix local indexes fail if the default_column_family is lower case
    public static final String COLUMN_FAMILY = "0";
    public static final byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);

    public static final String PHOENIX_LOCK_COLUMN = "PHOENIX_LOCK";
    public static final String PHOENIX_INDEX_LOCK_COLUMN = "PHOENIX_INDEX_LOCK";

    private final int chunkSize;

    private final Configuration conf;

    private int studyId;

    protected GenomeHelper(GenomeHelper other, int studyId) {
        this(other.getConf(), studyId);
    }

    protected GenomeHelper(Configuration conf) {
        this(conf, conf.getInt(HadoopVariantStorageEngine.STUDY_ID, -1));
    }

    protected GenomeHelper(Configuration conf, int studyId) {
        this.conf = conf;
        // TODO: Check if columnFamily is upper case
        // Phoenix local indexes fail if the default_column_family is lower case
        // TODO: Report this bug to phoenix JIRA
        this.chunkSize = conf.getInt(ARCHIVE_CHUNK_SIZE.key(), ARCHIVE_CHUNK_SIZE.defaultValue());
        this.studyId = studyId > 0 ? studyId : getStudyId(conf);
    }

    public static int getStudyId(Configuration conf) {
        return conf.getInt(HadoopVariantStorageEngine.STUDY_ID, -1);
    }

    public Configuration getConf() {
        return conf;
    }

    public static void setChunkSize(Configuration conf, Integer size) {
        conf.setInt(ARCHIVE_CHUNK_SIZE.key(), size);
    }

    public static void setStudyId(Configuration conf, Integer studyId) {
        conf.setInt(HadoopVariantStorageEngine.STUDY_ID, studyId);
    }

    protected void setStudyId(int studyId) {
        this.studyId = studyId;
    }

    public int getStudyId() {
        return this.studyId;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * TODO: Query CellBase to get the chromosomes and sizes!
     * @param numberOfSplits    Number of splits
     * @param keyGenerator      Function to generate the rowKeys given a chromosome and a start
     * @return                  List of splits
     */
    public static List<byte[]> generateBootPreSplitsHuman(int numberOfSplits, BiFunction<String, Integer, byte[]> keyGenerator) {
        String[] chr = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20", "21", "22", "X", "Y", };
        long[] posarr = new long[]{249250621, 243199373, 198022430, 191154276, 180915260, 171115067, 159138663,
                146364022, 141213431, 135534747, 135006516, 133851895, 115169878, 107349540, 102531392, 90354753,
                81195210, 78077248, 59128983, 63025520, 48129895, 51304566, 155270560, 59373566, };
        Map<String, Long> regions = new HashMap<>();
        for (int i = 0; i < chr.length; i++) {
            regions.put(chr[i], posarr[i]);
        }
        return generateBootPreSplits(numberOfSplits, keyGenerator, regions);
    }

    static List<byte[]> generateBootPreSplits(int numberOfSplits, BiFunction<String, Integer, byte[]> keyGenerator,
                                              Map<String, Long> regionsMap) {
        // Create a sorted map for the regions that sorts as will sort HBase given the row_key generator
        // In archive table, chr1 goes after chr19, and in Variants table, chr1 is always the first
        SortedMap<String, Long> sortedRegions = new TreeMap<>((s1, s2) ->
                Bytes.compareTo(keyGenerator.apply(s1, 0), keyGenerator.apply(s2, 0)));
        sortedRegions.putAll(regionsMap);

        long total = sortedRegions.values().stream().reduce((a, b) -> a + b).orElse(0L);
        long chunkSize = total / numberOfSplits;
        List<byte[]> splitList = new ArrayList<>();
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
            byte[] rowKey = keyGenerator.apply(chr, (int) (splitPos - tmpPos));
            splitList.add(rowKey);
            splitPos += chunkSize;
        }
        return splitList;
    }

    public <T extends MessageLite> Put wrapAsPut(byte[] column, byte[] row, T meta) {
        byte[] data = meta.toByteArray();
        Put put = new Put(row);
        put.addColumn(COLUMN_FAMILY_BYTES, column, data);
        return put;
    }

}
