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

package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created on 20/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GenomeHelperTest {

    public static final int CHUNK_SIZE = 200;
    private GenomeHelper genomeHelper;
    private ArchiveRowKeyFactory keyFactory;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        GenomeHelper.setChunkSize(conf, CHUNK_SIZE);
        genomeHelper = new GenomeHelper(conf);

        keyFactory = new ArchiveRowKeyFactory(CHUNK_SIZE, '_', 100);
    }

    @Test
    public void testBlockRowKey() throws Exception {
        Assert.assertEquals("2", keyFactory.extractChromosomeFromBlockId("0001_2_00000222"));
        // Parse complex contigs
        Assert.assertEquals("NC_007605", keyFactory.extractChromosomeFromBlockId("0001_NC_007605_00000222"));
        Assert.assertEquals(1, keyFactory.extractFileBatchFromBlockId("0001_NC_007605_00000222"));
        Assert.assertEquals(222, keyFactory.extractSliceFromBlockId("0001_2_222"));
        Assert.assertEquals(222 * CHUNK_SIZE, keyFactory.extractPositionFromBlockId("0001_2_222"));
        Assert.assertEquals(0, keyFactory.getFileBatch(1));
        Assert.assertEquals(0, keyFactory.getFileBatch(99));
        Assert.assertEquals(1, keyFactory.getFileBatch(100));
        Assert.assertEquals(1, keyFactory.getFileBatch(101));
    }

    @Test
    public void testVariantRowKey() throws Exception {
        checkVariantRowKeyGeneration(new Variant("5", 21648, "A", "T"));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "AAAAAA", "T"));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "A", ""));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "AAT", "TTT"));
        checkVariantRowKeyGeneration(new Variant("X", 21648, "", "TTT"));
        checkVariantRowKeyGeneration(new Variant("MT", 21648, "", ""));
    }

    public void checkVariantRowKeyGeneration(Variant variant) {
        byte[] variantRowkey = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
        Variant generatedVariant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(variantRowkey);
        byte[] phoenixRowKey = generateVariantRowKeyPhoenix(variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate());
        assertArrayEquals(variant.toString(), phoenixRowKey, variantRowkey);
        assertEquals(variant, generatedVariant);
    }

    @Test
    public void testGenerateSplitArchive() throws Exception {
        assertOrder(GenomeHelper.generateBootPreSplitsHuman(30, (chr, pos) -> keyFactory.generateBlockIdAsBytes(1, chr, pos)), 30);
    }

    @Test
    public void testGenerateSplitArchiveMultipleBatches() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(HadoopVariantStorageEngine.ARCHIVE_TABLE_PRESPLIT_SIZE, 10);
        conf.setInt(HadoopVariantStorageEngine.EXPECTED_FILES_NUMBER, 4500);
        conf.setInt(HadoopVariantStorageEngine.ARCHIVE_FILE_BATCH_SIZE, 1000);
        assertOrder(ArchiveTableHelper.generateArchiveTableBootPreSplitHuman(conf), 50);
    }

    @Test
    public void testGenerateSplitVariants() throws Exception {
        assertOrder(GenomeHelper.generateBootPreSplitsHuman(30, VariantPhoenixKeyFactory::generateVariantRowKey), 30);
    }

    void assertOrder(List<byte[]> bytes, int expectedSize) {
        String prev = "0";
        for (byte[] bytesKey : bytes) {
            String key = new String(bytesKey);
            if (StringUtils.isAsciiPrintable(key)) {
                System.out.println("key = " + key);
            } else {
                System.out.println("key = " + Bytes.toHex(bytesKey));
            }
//            System.out.println(prev + ".compareTo(" + key + ") = " + prev.compareTo(key));
            assertTrue(prev.compareTo(key) < 0);
            prev = key;
        }
        assertEquals(expectedSize, bytes.size());
    }

    public byte[] generateVariantRowKeyPhoenix(String chrom, int position, String ref, String alt) {
        PTableImpl table;
        try {
            List<PColumn> columns = Arrays.asList(
                    PColumnImpl.createFromProto(PTableProtos.PColumn.newBuilder().setColumnNameBytes(ByteStringer.wrap(PNameFactory.newName("CHROMOSOME").getBytes())).setDataType(PVarchar.INSTANCE.getSqlTypeName()).setPosition(0).setNullable(false).setSortOrder(SortOrder.ASC.getSystemValue()).build()),
                    PColumnImpl.createFromProto(PTableProtos.PColumn.newBuilder().setColumnNameBytes(ByteStringer.wrap(PNameFactory.newName("POSITION").getBytes())).setDataType(PUnsignedInt.INSTANCE.getSqlTypeName()).setPosition(1).setNullable(false).setSortOrder(SortOrder.ASC.getSystemValue()).build()),
                    PColumnImpl.createFromProto(PTableProtos.PColumn.newBuilder().setColumnNameBytes(ByteStringer.wrap(PNameFactory.newName("REFERENCE").getBytes())).setDataType(PVarchar.INSTANCE.getSqlTypeName()).setPosition(2).setNullable(true).setSortOrder(SortOrder.ASC.getSystemValue()).build()),
                    PColumnImpl.createFromProto(PTableProtos.PColumn.newBuilder().setColumnNameBytes(ByteStringer.wrap(PNameFactory.newName("ALTERNATE").getBytes())).setDataType(PVarchar.INSTANCE.getSqlTypeName()).setPosition(3).setNullable(true).setSortOrder(SortOrder.ASC.getSystemValue()).build())
            );
            table = PTableImpl.makePTable(new PTableImpl(), columns);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        ImmutableBytesWritable key = new ImmutableBytesWritable();
        table.newKey(key, new byte[][]{Bytes.toBytes(chrom), Bytes.toBytes(position), Bytes.toBytes(ref), Bytes.toBytes(alt)});

        if (key.getLength() == key.get().length) {
            return key.get();
        } else {
            return Arrays.copyOf(key.get(), key.getLength());
        }
    }

}
