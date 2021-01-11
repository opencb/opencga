package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;

import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created on 25/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixKeyFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testVariantRowKey() throws Exception {
        checkVariantRowKeyGeneration(new Variant("5", 21648, "A", "T"));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "AAAAAA", "T"));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "A", ""));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "AAT", "TTT"));
        checkVariantRowKeyGeneration(new Variant("X", 21648, "", "TTT"));
        checkVariantRowKeyGeneration(new Variant("MT", 21648, "", ""));
    }

    @Test
    public void testStructuralVariantRowKey() throws Exception {
        checkVariantRowKeyGeneration(new Variant("5:110-510:-:<DEL>"));
        checkVariantRowKeyGeneration(new Variant("5:100:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:-"));
        checkVariantRowKeyGeneration(new Variant("5:100<110<120-500<510<520:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:-"));
        checkVariantRowKeyGeneration(new Variant("5:100<110<120-500<510<520:A:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"));
        checkVariantRowKeyGeneration(new Variant("5:100<110<120-500<510<520:A:<DEL>"));
        checkVariantRowKeyGeneration(new Variant("5:100<110<120-500<510<520:-:<DEL>"));
        checkVariantRowKeyGeneration(new Variant("5:100<110<120-500<510<520:A:<CN5>"));
        checkVariantRowKeyGeneration(new Variant("5:100<110<120-500<510<520:-:<CN5>"));
        checkVariantRowKeyGeneration(new Variant("5:100:A:A]:chr5:234]"));
    }

    @Test
    public void testExtractChrPosFromVariantRowKeyPartial() {
        byte[] phoenixRowKey = VariantPhoenixKeyFactory.generateVariantRowKey("1", 20 << 16);
        Pair<String, Integer> expected = VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey(phoenixRowKey, 0, phoenixRowKey.length, false);

        phoenixRowKey = Arrays.copyOf(phoenixRowKey, phoenixRowKey.length - 1); // Remove leading zero
        Pair<String, Integer> actual = VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey(phoenixRowKey, 0, phoenixRowKey.length, true);
        assertEquals(expected, actual);

        thrown.expect(RuntimeException.class);
        VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey(phoenixRowKey, 0, phoenixRowKey.length, false);
    }

    public void checkVariantRowKeyGeneration(Variant variant) {
        byte[] phoenixRowKey = generateVariantRowKeyPhoenix(variant);

//        System.out.println("expected = " + Bytes.toStringBinary(phoenixRowKey));

        byte[] variantRowkey = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
//        System.out.println("actual   = " + Bytes.toStringBinary(variantRowkey));
        Variant generatedVariant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(variantRowkey);

        assertArrayEquals(variant.toString(), phoenixRowKey, variantRowkey);
        assertEquals(variant, generatedVariant);
    }

    public byte[] generateVariantRowKeyPhoenix(Variant variant) {

        Set<VariantPhoenixSchema.VariantColumn> nullableColumn = new HashSet<>(Arrays.asList(
                VariantPhoenixSchema.VariantColumn.REFERENCE,
                VariantPhoenixSchema.VariantColumn.ALTERNATE
        ));

        PTableImpl table;
        try {
            List<PColumn> columns = new ArrayList<>();
            for (PhoenixHelper.Column column : VariantPhoenixSchema.PRIMARY_KEY) {
                columns.add(PColumnImpl.createFromProto(PTableProtos.PColumn.newBuilder()
                        .setColumnNameBytes(ByteStringer.wrap(PNameFactory.newName(column.column()).getBytes()))
                        .setDataType(column.getPDataType().getSqlTypeName())
                        .setPosition(columns.size())
                        .setNullable(nullableColumn.contains(column))
                        .setSortOrder(SortOrder.ASC.getSystemValue()).build()));
            }

            table = PTableImpl.makePTable(new PTableImpl(), columns);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        ImmutableBytesWritable key = new ImmutableBytesWritable();
        table.newKey(key, new byte[][]{
                Bytes.toBytes(variant.getChromosome()),
                Bytes.toBytes(variant.getStart()),
                Bytes.toBytes(variant.getReference()),
                Bytes.toBytes(VariantPhoenixKeyFactory.buildSymbolicAlternate(variant.getReference(), variant.getAlternate(), variant.getEnd(), variant.getSv())),
        });

        if (key.getLength() == key.get().length) {
            return key.get();
        } else {
            return Arrays.copyOf(key.get(), key.getLength());
        }
    }
}