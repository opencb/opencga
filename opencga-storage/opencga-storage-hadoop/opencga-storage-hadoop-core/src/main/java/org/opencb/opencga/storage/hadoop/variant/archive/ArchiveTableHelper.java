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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice.Builder;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class ArchiveTableHelper extends GenomeHelper {

    private final Logger logger = LoggerFactory.getLogger(ArchiveTableHelper.class);
    private final AtomicReference<VcfMeta> meta = new AtomicReference<>();
    private final ArchiveRowKeyFactory keyFactory;
    private byte[] column;

    private final VcfRecordComparator vcfComparator = new VcfRecordComparator();

    public ArchiveTableHelper(Configuration conf) throws IOException {
        this(conf, null);
        int fileId = conf.getInt(VariantStorageEngine.Options.FILE_ID.key(), 0);
        int studyId = conf.getInt(VariantStorageEngine.Options.STUDY_ID.key(), 0);
        try (HadoopVariantSourceDBAdaptor metadataManager = new HadoopVariantSourceDBAdaptor(conf)) {
            VcfMeta meta = metadataManager.getVcfMeta(getStudyId(), fileId, null);
            this.meta.set(meta);
        }
        column = Bytes.toBytes(getColumnName(meta.get().getVariantSource()));
    }

    public ArchiveTableHelper(GenomeHelper helper, VcfMeta meta) {
        super(helper);
        this.meta.set(meta);
        column = Bytes.toBytes(getColumnName(meta.getVariantSource()));
        keyFactory = new ArchiveRowKeyFactory(getChunkSize(), getSeparator());
    }

    public ArchiveTableHelper(Configuration conf, VcfMeta meta) {
        super(conf);
        if (meta != null) {
            this.meta.set(meta);
            VariantSource variantSource = getMeta().getVariantSource();
            column = Bytes.toBytes(getColumnName(variantSource));
        }
        keyFactory = new ArchiveRowKeyFactory(getChunkSize(), getSeparator());
    }

    public ArchiveTableHelper(GenomeHelper helper, VariantSource source) {
        super(helper);
        this.meta.set(new VcfMeta(source));
        column = Bytes.toBytes(getColumnName(source));
        keyFactory = new ArchiveRowKeyFactory(getChunkSize(), getSeparator());
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
    public static String getColumnName(int fileId) {
        return Integer.toString(fileId);
    }

    /**
     * Get the archive column name for a file given a FileId.
     *
     * @param columnName Column name
     * @return Related fileId
     */
    public static int getFileIdFromColumnName(byte[] columnName) {
        return Integer.parseInt(Bytes.toString(columnName));
    }

    /**
     * Get the archive column name for a file given a VariantSource.
     *
     * @param variantSource VariantSource
     * @return Column name or Qualifier
     */
    public static String getColumnName(VariantSource variantSource) {
        return variantSource.getFileId();
    }

    public static boolean createArchiveTableIfNeeded(GenomeHelper genomeHelper, String tableName) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(genomeHelper.getConf())) {
            return createArchiveTableIfNeeded(genomeHelper, tableName, con);
        }
    }

    public static boolean createArchiveTableIfNeeded(GenomeHelper genomeHelper, String tableName, Connection con) throws IOException {
        Compression.Algorithm compression = Compression.getCompressionAlgorithmByName(
                genomeHelper.getConf().get(HadoopVariantStorageEngine.ARCHIVE_TABLE_COMPRESSION, Compression.Algorithm.SNAPPY.getName()));
        int nSplits = genomeHelper.getConf().getInt(HadoopVariantStorageEngine.ARCHIVE_TABLE_PRESPLIT_SIZE, 100);
        ArchiveRowKeyFactory rowKeyFactory = new ArchiveRowKeyFactory(genomeHelper.getChunkSize(), genomeHelper.getSeparator());
        List<byte[]> preSplits = generateBootPreSplitsHuman(nSplits, rowKeyFactory::generateBlockIdAsBytes);
        return HBaseManager.createTableIfNeeded(con, tableName, genomeHelper.getColumnFamily(), preSplits, compression);
    }

    public VcfMeta getMeta() {
        return meta.get();
    }

    public byte[] getColumn() {
        return column;
    }

    @Deprecated
    public VcfSlice join(byte[] key, Iterable<VcfSlice> input) throws InvalidProtocolBufferException {
        Builder sliceBuilder = VcfSlice.newBuilder();
        boolean isFirst = true;
        List<VcfRecord> vcfRecordLst = new ArrayList<VcfRecord>();
        for (VcfSlice slice : input) {

            byte[] skey = getKeyFactory().generateBlockIdAsBytes(slice.getChromosome(), slice.getPosition());
            // Consistency check
            if (!Bytes.equals(skey, key)) { // Address doesn't match up -> should never happen
                throw new IllegalStateException(String.format("Row keys don't match up!!! %s != %s", Bytes.toString(key),
                        Bytes.toString(skey)));
            }

            if (isFirst) { // init new slice
                sliceBuilder.setChromosome(slice.getChromosome()).setPosition(slice.getPosition());
                isFirst = false;
            }
            vcfRecordLst.addAll(slice.getRecordsList());
        }

        // Sort records
        try {
            Collections.sort(vcfRecordLst, getVcfComparator());
        } catch (IllegalArgumentException e) {
            logger.error("Issue with comparator: ");
            for (VcfRecord r : vcfRecordLst) {
                logger.error(r.toString());
            }
            throw e;
        }

        // Add all
        sliceBuilder.addAllRecords(vcfRecordLst);
        return sliceBuilder.build();
    }

    private VcfSlice extractSlice(Put put) throws InvalidProtocolBufferException {
        List<Cell> cList = put.get(getColumnFamily(), getColumn());
        if (cList.isEmpty()) {
            throw new IllegalStateException(String.format("No data available for row % in column %s in familiy %s!!!",
                    Bytes.toString(put.getRow()), Bytes.toString(getColumn()), Bytes.toString(getColumnFamily())));
        }
        if (cList.size() > 1) {
            throw new IllegalStateException(String.format("One entry instead of %s expected for row %s column %s in familiy %s!!!",
                    cList.size(), Bytes.toString(put.getRow()), Bytes.toString(getColumn()), Bytes.toString(getColumnFamily())));
        }
        Cell cell = cList.get(0);

        byte[] arr = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength());
        VcfSlice slice = VcfSlice.parseFrom(arr);
        return slice;
    }

    private VcfRecordComparator getVcfComparator() {
        return vcfComparator;
    }

    public byte[] wrap(VcfRecord record) {
        return record.toByteArray();
    }

    public Put wrap(VcfSlice slice) {
//        byte[] rowId = generateBlockIdAsBytes(slice.getChromosome(), (long) slice.getPosition() + slice.getRecords(0).getRelativeStart
// () * 100);
        byte[] rowId = keyFactory.generateBlockIdAsBytes(slice.getChromosome(), slice.getPosition());
        return wrapAsPut(getColumn(), rowId, slice);
    }

}
