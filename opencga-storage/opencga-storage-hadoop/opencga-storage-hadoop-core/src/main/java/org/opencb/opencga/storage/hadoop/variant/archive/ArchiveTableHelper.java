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
package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantFileMetadataDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class ArchiveTableHelper extends GenomeHelper {

    public static final String NON_REF_COLUMN_SUFIX = "_N";
    public static final byte[] NON_REF_COLUMN_SUFIX_BYTES = Bytes.toBytes(NON_REF_COLUMN_SUFIX);
    public static final String REF_COLUMN_SUFIX = "_R";
    public static final byte[] REF_COLUMN_SUFIX_BYTES = Bytes.toBytes(REF_COLUMN_SUFIX);

    private final Logger logger = LoggerFactory.getLogger(ArchiveTableHelper.class);
    private final AtomicReference<VariantFileMetadata> meta = new AtomicReference<>();
    private final ArchiveRowKeyFactory keyFactory;
    private final byte[] nonRefColumn;
    private final byte[] refColumn;

    private final int fileId;

    @Deprecated
    public ArchiveTableHelper(Configuration conf) throws IOException {
        // FIXME: Read MetaTableName may fail!
        this(conf, new VariantTableHelper(conf).getMetaTableAsString());
    }

    public ArchiveTableHelper(Configuration conf, String metaTableName) throws IOException {
        super(conf);
        fileId = conf.getInt(HadoopVariantStorageEngine.FILE_ID, 0);

        try (HBaseVariantFileMetadataDBAdaptor metadataManager = new HBaseVariantFileMetadataDBAdaptor(null, metaTableName, conf)) {
            VariantFileMetadata meta = metadataManager.getVariantFileMetadata(getStudyId(), fileId, null);
            this.meta.set(meta);
            nonRefColumn = Bytes.toBytes(getNonRefColumnName(meta));
            refColumn = Bytes.toBytes(getRefColumnName(meta));
        }
        keyFactory = new ArchiveRowKeyFactory(conf);
    }

    public ArchiveTableHelper(GenomeHelper helper, int studyId, VariantFileMetadata meta) {
        super(helper, studyId);
        this.meta.set(meta);
        fileId = Integer.valueOf(meta.getId());
        nonRefColumn = Bytes.toBytes(getNonRefColumnName(meta));
        refColumn = Bytes.toBytes(getRefColumnName(meta));
        keyFactory = new ArchiveRowKeyFactory(helper.getConf());
    }

    public ArchiveTableHelper(Configuration conf, int studyId, VariantFileMetadata meta) {
        super(conf, studyId);
        this.meta.set(meta);
        fileId = Integer.valueOf(meta.getId());
        nonRefColumn = Bytes.toBytes(getNonRefColumnName(meta));
        refColumn = Bytes.toBytes(getRefColumnName(meta));
        keyFactory = new ArchiveRowKeyFactory(conf);
    }

    public ArchiveRowKeyFactory getKeyFactory() {
        return keyFactory;
    }

    /**
     * Get the archive column name for a file given a FileId.
     *
     * @param fileId Numerical file identifier
     * @return Column name or Qualifier
     */
    public static String getNonRefColumnName(int fileId) {
        return Integer.toString(fileId) + NON_REF_COLUMN_SUFIX;
    }

    public int getFileId() {
        return fileId;
    }

    /**
     * Get the archive column name for a file given a FileId.
     *
     * @param columnName Column name
     * @return Related fileId
     */
    public static int getFileIdFromNonRefColumnName(byte[] columnName) {
        return Integer.parseInt(Bytes.toString(columnName, 0, columnName.length - NON_REF_COLUMN_SUFIX.length()));
    }

    /**
     * Get the archive column name for a file given a FileId.
     *
     * @param columnName Column name
     * @return Related fileId
     */
    public static int getFileIdFromRefColumnName(byte[] columnName) {
        return Integer.parseInt(Bytes.toString(columnName, 0, columnName.length - REF_COLUMN_SUFIX.length()));
    }

    public static boolean isNonRefColumn(byte[] columnName) {
        return columnName.length > 0 && Character.isDigit(columnName[0]) && endsWith(columnName, NON_REF_COLUMN_SUFIX_BYTES);
    }

    public static boolean isRefColumn(byte[] columnName) {
        return columnName.length > 0 && Character.isDigit(columnName[0]) && endsWith(columnName, REF_COLUMN_SUFIX_BYTES);
    }

    private static boolean endsWith(byte[] columnName, byte[] sufixBytes) {
        return columnName.length > sufixBytes.length && Bytes.equals(
                columnName, columnName.length - sufixBytes.length, sufixBytes.length,
                sufixBytes, 0, sufixBytes.length
        );
    }


    /**
     * Get the archive column name for a file given a VariantFileMetadata.
     *
     * @param fileMetadata VariantFileMetadata
     * @return Column name or Qualifier
     */
    public static String getNonRefColumnName(VariantFileMetadata fileMetadata) {
        return getNonRefColumnName(Integer.parseInt(fileMetadata.getId()));
    }

    /**
     * Get the archive column name for a file given a VariantFileMetadata.
     *
     * @param fileMetadata VariantFileMetadata
     * @return Column name or Qualifier
     */
    public static String getRefColumnName(VariantFileMetadata fileMetadata) {
        return getRefColumnName(Integer.parseInt(fileMetadata.getId()));
    }
    /**
     * Get the archive column name for a file given a FileId.
     *
     * @param fileId Numerical file identifier
     * @return Column name or Qualifier
     */
    public static String getRefColumnName(int fileId) {
        return fileId + REF_COLUMN_SUFIX;
    }

    public static boolean createArchiveTableIfNeeded(GenomeHelper genomeHelper, String tableName) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(genomeHelper.getConf())) {
            return createArchiveTableIfNeeded(genomeHelper, tableName, con);
        }
    }

    public static boolean createArchiveTableIfNeeded(GenomeHelper genomeHelper, String tableName, Connection con) throws IOException {
        Compression.Algorithm compression = Compression.getCompressionAlgorithmByName(
                genomeHelper.getConf().get(HadoopVariantStorageEngine.ARCHIVE_TABLE_COMPRESSION, Compression.Algorithm.SNAPPY.getName()));
        final List<byte[]> preSplits = generateArchiveTableBootPreSplitHuman(genomeHelper.getConf());
        return HBaseManager.createTableIfNeeded(con, tableName, genomeHelper.getColumnFamily(), preSplits, compression);
    }

    public static List<byte[]> generateArchiveTableBootPreSplitHuman(Configuration conf) {
        ArchiveRowKeyFactory rowKeyFactory = new ArchiveRowKeyFactory(conf);

        int nSplits = conf.getInt(
                HadoopVariantStorageEngine.ARCHIVE_TABLE_PRESPLIT_SIZE,
                HadoopVariantStorageEngine.DEFAULT_ARCHIVE_TABLE_PRESPLIT_SIZE);
        int expectedNumBatches = rowKeyFactory.getFileBatch(conf.getInt(
                HadoopVariantStorageEngine.EXPECTED_FILES_NUMBER,
                HadoopVariantStorageEngine.DEFAULT_EXPECTED_FILES_NUMBER));

        final List<byte[]> preSplits = new ArrayList<>(nSplits * expectedNumBatches);
        for (int batch = 0; batch <= expectedNumBatches; batch++) {
            int finalBatch = batch;
            preSplits.addAll(generateBootPreSplitsHuman(nSplits, (chr, start) ->
                    Bytes.toBytes(rowKeyFactory.generateBlockIdFromSliceAndBatch(finalBatch, chr, start))));
        }
        return preSplits;
    }

    public VariantFileMetadata getFileMetadata() {
        return meta.get();
    }

    public VariantStudyMetadata getStudyMetadata() {
        return meta.get().toVariantStudyMetadata(String.valueOf(getStudyId()));
    }

    public byte[] getNonRefColumnName() {
        return nonRefColumn;
    }

    public byte[] getRefColumnName() {
        return refColumn;
    }

    @Deprecated
    public Put wrap(VcfSlice slice) {
        return wrap(slice, false);
    }

    public Put wrap(VcfSlice slice, boolean isRef) {
//        byte[] rowId = generateBlockIdAsBytes(slice.getChromosome(), (long) slice.getPosition() + slice.getRecords(0).getRelativeStart
// () * 100);
        byte[] rowId = keyFactory.generateBlockIdAsBytes(getFileId(), slice.getChromosome(), slice.getPosition());
        return wrapAsPut(isRef ? getRefColumnName() : getNonRefColumnName(), rowId, slice);
    }

}
