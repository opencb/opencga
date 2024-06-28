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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseFileMetadataDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.*;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class ArchiveTableHelper extends GenomeHelper {

    public static final String NON_REF_COLUMN_SUFIX = "_N";
    public static final byte[] NON_REF_COLUMN_SUFIX_BYTES = Bytes.toBytes(NON_REF_COLUMN_SUFIX);
    public static final String REF_COLUMN_SUFIX = "_R";
    public static final byte[] REF_COLUMN_SUFIX_BYTES = Bytes.toBytes(REF_COLUMN_SUFIX);
    public static final String CONFIG_ARCHIVE_TABLE_NAME          = "opencga.archive.table.name";

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

        try (HBaseFileMetadataDBAdaptor metadataManager = new HBaseFileMetadataDBAdaptor(null, metaTableName, conf)) {
            VariantFileMetadata meta = metadataManager.getVariantFileMetadata(getStudyId(), fileId, null).first();
            this.meta.set(meta);
            nonRefColumn = Bytes.toBytes(getNonRefColumnName(meta));
            refColumn = Bytes.toBytes(getRefColumnName(meta));
        } catch (StorageEngineException e) {
            throw new IOException(e);
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

    public static boolean createArchiveTableIfNeeded(Configuration conf, String tableName) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(conf)) {
            return createArchiveTableIfNeeded(conf, tableName, con);
        }
    }

    public static boolean createArchiveTableIfNeeded(Configuration conf, String tableName, Connection con) throws IOException {
        Compression.Algorithm compression = Compression.getCompressionAlgorithmByName(
                conf.get(ARCHIVE_TABLE_COMPRESSION.key(), ARCHIVE_TABLE_COMPRESSION.defaultValue()));
        final List<byte[]> preSplits = generateArchiveTableBootPreSplitHuman(conf);
        return HBaseManager.createTableIfNeeded(con, tableName, COLUMN_FAMILY_BYTES, preSplits, compression);
    }

    public static boolean createArchiveTableIfNeeded(ObjectMap conf, String tableName, Connection con) throws IOException {
        Compression.Algorithm compression = Compression.getCompressionAlgorithmByName(
                conf.getString(ARCHIVE_TABLE_COMPRESSION.key(), ARCHIVE_TABLE_COMPRESSION.defaultValue()));
        final List<byte[]> preSplits = generateArchiveTableBootPreSplitHuman(conf);
        return HBaseManager.createTableIfNeeded(con, tableName, COLUMN_FAMILY_BYTES, preSplits, compression);
    }

    public static List<byte[]> generateArchiveTableBootPreSplitHuman(Configuration conf) {
        final ArchiveRowKeyFactory rowKeyFactory = new ArchiveRowKeyFactory(conf);
        int nSplits = conf.getInt(ARCHIVE_TABLE_PRESPLIT_SIZE.key(), ARCHIVE_TABLE_PRESPLIT_SIZE.defaultValue());
        int expectedFiles = conf.getInt(EXPECTED_FILES_NUMBER.key(), EXPECTED_FILES_NUMBER.defaultValue());
        return generateArchiveTableBootPreSplitHuman(rowKeyFactory, nSplits, expectedFiles);
    }

    public static List<byte[]> generateArchiveTableBootPreSplitHuman(ObjectMap conf) {
        final ArchiveRowKeyFactory rowKeyFactory = new ArchiveRowKeyFactory(conf);
        int nSplits = conf.getInt(ARCHIVE_TABLE_PRESPLIT_SIZE.key(), ARCHIVE_TABLE_PRESPLIT_SIZE.defaultValue());
        int expectedFiles = conf.getInt(EXPECTED_FILES_NUMBER.key(), EXPECTED_FILES_NUMBER.defaultValue());
        return generateArchiveTableBootPreSplitHuman(rowKeyFactory, nSplits, expectedFiles);
    }

    private static List<byte[]> generateArchiveTableBootPreSplitHuman(ArchiveRowKeyFactory rowKeyFactory, int nSplits, int expectedFiles) {
        int expectedNumBatches = rowKeyFactory.getFileBatch(
                expectedFiles);

        final List<byte[]> preSplits = new ArrayList<>(nSplits * expectedNumBatches);
        for (int batch = 0; batch <= expectedNumBatches; batch++) {
            int finalBatch = batch;
            preSplits.addAll(generateBootPreSplitsHuman(nSplits, (chr, position) -> {
                long slice = rowKeyFactory.getSliceId(position);
                return Bytes.toBytes(rowKeyFactory.generateBlockIdFromSliceAndBatch(finalBatch, chr, slice));
            }));
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
