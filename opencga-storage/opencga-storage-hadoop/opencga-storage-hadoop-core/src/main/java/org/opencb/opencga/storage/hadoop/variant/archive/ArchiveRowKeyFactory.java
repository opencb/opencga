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
import org.opencb.biodata.models.variant.Variant;

import static org.opencb.opencga.storage.hadoop.variant.GenomeHelper.DEFAULT_ROWKEY_SEPARATOR;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;

/**
 * Created on 25/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ArchiveRowKeyFactory {

    private final int chunkSize;
    private final char separator;
    private final int fileBatchSize;

    private static final int FILE_BATCH_IDX = 0;
    private static final int CHROMOSOME_IDX = 1;
    private static final int SLICE_IDX = 2;
    private static final int FIELDS_COUNT = 3;

    private static final int FILE_BATCH_PAD = 5;
    private static final int POSITION_PAD = 12;

    public ArchiveRowKeyFactory(Configuration conf) {
        this.chunkSize = conf.getInt(ARCHIVE_CHUNK_SIZE, DEFAULT_ARCHIVE_CHUNK_SIZE);
        this.separator = conf.get(ARCHIVE_ROW_KEY_SEPARATOR, DEFAULT_ROWKEY_SEPARATOR).charAt(0);
        this.fileBatchSize = conf.getInt(ARCHIVE_FILE_BATCH_SIZE, DEFAULT_ARCHIVE_FILE_BATCH_SIZE);
    }

    public ArchiveRowKeyFactory(int chunkSize, char separator, int fileBatchSize) {
        this.chunkSize = chunkSize;
        this.separator = separator;
        this.fileBatchSize = fileBatchSize;
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

    public int getFileBatch(int fileId) {
        if (fileId <= 0) {
            throw new IllegalArgumentException("FileId must be greater than 0. Got " + fileId);
        }
        return fileId / fileBatchSize;
    }

    public String generateBlockId(Variant variant, int fileId) {
        return generateBlockIdFromSlice(fileId, variant.getChromosome(), getSliceId(variant.getStart()));
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
     * @param fileId   File id
     * @param chrom    Chromosome name
     * @param position Genomic position
     * @return {@link String} Row key string
     */
    public String generateBlockId(int fileId, String chrom, long position) {
        return generateBlockIdFromSliceAndBatch(getFileBatch(fileId), chrom, getSliceId(position));
    }

    public String generateBlockIdFromSlice(int fileId, String chrom, long slice) {
        return generateBlockIdFromSliceAndBatch(getFileBatch(fileId), chrom, slice);
    }
    public String generateBlockIdFromSliceAndBatch(int fileBatch, String chrom, long slice) {
        String chromosome = Region.normalizeChromosome(chrom);
        StringBuilder sb = new StringBuilder(FILE_BATCH_PAD + 1 + chromosome.length() + 1 + POSITION_PAD);
        sb.append(StringUtils.leftPad(String.valueOf(fileBatch), FILE_BATCH_PAD, '0'));
        sb.append(getSeparator());
        sb.append(chromosome);
        sb.append(getSeparator());
        sb.append(StringUtils.leftPad(String.valueOf(slice), POSITION_PAD, '0'));
        return sb.toString();
    }

    /**
     * Changes the String from {@link #generateBlockId(int, String, long)} to bytes.
     *
     * @param fileId File id
     * @param chrom  Chromosome
     * @param start  Position
     * @return {@link Byte} array
     */
    public byte[] generateBlockIdAsBytes(int fileId, String chrom, int start) {
        return Bytes.toBytes(generateBlockId(fileId, chrom, start));
    }

    public int extractFileBatchFromBlockId(String blockId) {
        return Integer.valueOf(splitBlockId(blockId)[FILE_BATCH_IDX]);
    }

    public String extractChromosomeFromBlockId(String blockId) {
        return extractChromosomeFromBlockId(splitBlockId(blockId));
    }

    public String extractChromosomeFromBlockId(String[] strings) {
        return strings[CHROMOSOME_IDX];
    }

    public long extractSliceFromBlockId(String blockId) {
        return Long.parseLong(splitBlockId(blockId)[SLICE_IDX]);
    }

    public long getStartPositionFromSlice(long slice) {
        return slice * (long) getChunkSize();
    }

    public long extractPositionFromBlockId(String blockId) {
        return extractPositionFromBlockId(splitBlockId(blockId));
    }

    public long extractPositionFromBlockId(String[] strings) {
        return Long.parseLong(strings[SLICE_IDX]) * getChunkSize();
    }

    public Region extractRegionFromBlockId(String blockId) {
        String[] split = splitBlockId(blockId);
        String chr = extractChromosomeFromBlockId(split);
        long position = extractPositionFromBlockId(split);

        return new Region(chr, (int) position, (int) (position + getChunkSize()));
    }

    public String[] splitBlockId(String blockId) {
        char sep = getSeparator();
        String[] split = StringUtils.splitPreserveAllTokens(blockId, sep);

        if (split.length < FIELDS_COUNT) {
            throw new IllegalStateException(
                    String.format("Block ID is not valid - expected 2 or more blocks separated by `%s`; value `%s`", sep, blockId));
        }

        if (split.length > FIELDS_COUNT) {
            // Should parse contigs with separator in names, e.g. NC_007605
            StringBuilder contig = new StringBuilder();
            contig.append(split[CHROMOSOME_IDX]);
            for (int i = CHROMOSOME_IDX + 1; i < split.length - 1; i++) {
                contig.append(sep);
                contig.append(split[i]);
            }
            return new String[]{split[FILE_BATCH_IDX], contig.toString(), split[split.length - 1]};
        } else {
            return split;
        }
    }

}
