package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created on 25/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class VariantPhoenixKeyFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testVariantRowKey() throws Exception {
        checkVariantRowKeyGeneration(new Variant("5", 21648, "A", "T"));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "AAAAAA", "T"));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "A", ""));
        checkVariantRowKeyGeneration(new Variant("5", 21648, "", "T"));
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
    public void testExtraLargeVariantRowKey() throws Exception {
        String allele1 = RandomStringUtils.random(50000, "ACGT");
        checkVariantRowKeyGeneration(new Variant("5:1000:-:" + allele1));
        StructuralVariation sv = new StructuralVariation();
        sv.setLeftSvInsSeq(allele1);
        sv.setRightSvInsSeq(allele1);
        checkVariantRowKeyGeneration(new Variant("5:1000:A:<INS>").setSv(sv));
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
        byte[] alleles = Bytes.toBytes(VariantPhoenixKeyFactory.buildAlleles(variant));
//        System.out.println("actual   = " + Bytes.toStringBinary(variantRowkey));
        Variant generatedVariant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(variantRowkey, null, alleles);

        Result result = Result.create(Collections.singletonList(new KeyValue(phoenixRowKey, GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.ALLELES.bytes(),
                alleles)));

        Variant generatedVariant2 = VariantPhoenixKeyFactory.extractVariantFromResult(result);

        assertArrayEquals(variant.toString(), phoenixRowKey, variantRowkey);
        assertEquals(variant, generatedVariant);
        assertEquals(variant, generatedVariant2);
    }

    public byte[] generateVariantRowKeyPhoenix(Variant variant) {

        Set<PhoenixHelper.Column> nullableColumn = new HashSet<>(Arrays.asList(
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
        String reference = variant.getReference();
        String alternate = VariantPhoenixKeyFactory.buildSymbolicAlternate(reference, variant.getAlternate(), variant.getEnd(), variant.getSv());
        table.newKey(key, new byte[][]{
                Bytes.toBytes(variant.getChromosome()),
                Bytes.toBytes(variant.getStart()),
                Bytes.toBytes(reference),
                Bytes.toBytes(alternate),
        });
        if (key.getLength() > HConstants.MAX_ROW_LENGTH) {
            key = new ImmutableBytesWritable();
            table.newKey(key, new byte[][]{
                    Bytes.toBytes(variant.getChromosome()),
                    Bytes.toBytes(variant.getStart()),
                    Bytes.toBytes(VariantPhoenixKeyFactory.hashAllele(reference)),
                    Bytes.toBytes(VariantPhoenixKeyFactory.hashAllele(alternate)),
            });
        }

        if (key.getLength() == key.get().length) {
            return key.get();
        } else {
            return Arrays.copyOf(key.get(), key.getLength());
        }
    }
}