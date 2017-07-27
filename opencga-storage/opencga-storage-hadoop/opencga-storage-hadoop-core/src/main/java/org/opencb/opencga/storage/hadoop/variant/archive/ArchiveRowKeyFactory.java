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

package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;

import static org.opencb.opencga.storage.hadoop.variant.GenomeHelper.DEFAULT_ROWKEY_SEPARATOR;

/**
 * Created on 25/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ArchiveRowKeyFactory {

    private final int chunkSize;
    private final char separator;

    public ArchiveRowKeyFactory(Configuration conf) {
        this.chunkSize = conf.getInt(HadoopVariantStorageEngine.ARCHIVE_CHUNK_SIZE, HadoopVariantStorageEngine.DEFAULT_ARCHIVE_CHUNK_SIZE);
        this.separator = conf.get(HadoopVariantStorageEngine.ARCHIVE_ROW_KEY_SEPARATOR, DEFAULT_ROWKEY_SEPARATOR).charAt(0);
    }

    public ArchiveRowKeyFactory(int chunkSize, char separator) {
        this.chunkSize = chunkSize;
        this.separator = separator;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public char getSeparator() {
        return separator;
    }

    public long getSliceId(long position) {
        return chunkSize > 0
                ? position / (long) chunkSize
                : position;
    }

    /**
     * Generates a Row key based on Chromosome and position adjusted for the
     * Chunk size. <br>
     * <ul>
     * <li>Using {@link Region#normalizeChromosome(String)} to get standard chromosome
     * name
     * <li>Using {@link #getSliceId(long)} to return slice position
     * </ul>
     * e.g. using chunk size 100, separator _ with chr2 and 1234 would result in
     * 2_12
     *
     * @param chrom    Chromosome name
     * @param position Genomic position
     * @return {@link String} Row key string
     */
    public String generateBlockId(String chrom, long position) {
        return generateBlockIdFromSlice(chrom, getSliceId(position));
    }

    public String generateBlockIdFromSlice(String chrom, long slice) {
        StringBuilder sb = new StringBuilder(Region.normalizeChromosome(chrom));
        sb.append(getSeparator());
        sb.append(String.format("%012d", slice));
        return sb.toString();
    }

    /**
     * Changes the String from {@link #generateBlockId(String, long)} to bytes.
     *
     * @param chrom Chromosome
     * @param start Position
     * @return {@link Byte} array
     */
    public byte[] generateBlockIdAsBytes(String chrom, int start) {
        return Bytes.toBytes(generateBlockId(chrom, start));
    }

    public String extractChromosomeFromBlockId(String blockId) {
        return extractChromosomeFromBlockId(splitBlockId(blockId));
    }

    public String extractChromosomeFromBlockId(String[] strings) {
        return strings[0];
    }

    public Long extractSliceFromBlockId(String blockId) {
        return Long.valueOf(splitBlockId(blockId)[1]);
    }

    public long getStartPositionFromSlice(long slice) {
        return slice * (long) getChunkSize();
    }

    public Long extractPositionFromBlockId(String blockId) {
        return Long.valueOf(splitBlockId(blockId)[1]) * getChunkSize();
    }

    public String[] splitBlockId(String blockId) {
        char sep = getSeparator();
        String[] split = StringUtils.splitPreserveAllTokens(blockId, sep);

        if (split.length < 2) {
            throw new IllegalStateException(
                    String.format("Block ID is not valid - expected 2 or more blocks separated by `%s`; value `%s`", sep, blockId));
        }

        // Should parse contigs with separator in names, e.g. NC_007605
        StringBuilder contig = new StringBuilder();
        for (int i = 0; i < split.length - 1; i++) {
            contig.append(split[i]);
            if (i < split.length - 2) {
                contig.append(String.valueOf(sep));
            }
        }

        String[] res = {contig.toString(), split[split.length - 1]};
        return res;
    }

}
